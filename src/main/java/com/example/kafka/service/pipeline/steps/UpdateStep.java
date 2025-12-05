package com.example.kafka.service.pipeline.steps;

import java.util.List;
import java.util.Objects;

import com.example.kafka.service.config.TransformProperties;
import com.example.kafka.service.pipeline.TransformContext;
import com.example.kafka.service.pipeline.TransformStep;

/**
 * UpdateStep:
 *  - stripCr: καθαρίζει \r και trim από συγκεκριμένα πεδία
 *  - compute: αξιολογεί "mini-expressions" και γράφει νέα πεδία στο context
 *
 * Υποστηρίζει εκφράσεις τύπου:
 *   expr: "${resolvedAt == placeholder ? 'Open' : 'Closed'}"
 *   expr: "nspFaultAlarmDelete != null ? 'CLEAR' :
 *          (fdn != null ? 'FAULT_SYNC' :
 *          (objectId != null ? 'FAULT' : 'UNKNOWN'))"
 * και απλό:
 *   expr: "someVar"
 * => παίρνει ctx.get("someVar")
 *
 * Επιπλέον υποστηρίζει:
 *   expr: "UPPER(severity)"
 *   expr: "UPPER(someVar)"
 */
public class UpdateStep implements TransformStep {

  private final List<String> stripCr;
  private final List<TransformProperties.ComputeAssignment> compute;
  private final String placeholder;

  public UpdateStep(List<String> stripCr,
                    List<TransformProperties.ComputeAssignment> compute,
                    String placeholder) {
    this.stripCr = stripCr;
    this.compute = compute;
    this.placeholder = placeholder;
  }

  @Override
  public void apply(TransformContext ctx) {
    // 1) Καθάρισμα \r και trim σε συγκεκριμένα fields
    if (stripCr != null) {
      for (String k : stripCr) {
        Object v = ctx.get(k);
        if (v instanceof String s) {
          ctx.put(k, s.replace("\r", "").trim());
        }
      }
    }

    // 2) Υπολογισμός compute expressions
    if (compute != null) {
      for (var c : compute) {
        String val = evalMiniExpr(c.getExpr(), ctx, placeholder);
        ctx.put(c.getSet(), val);
      }
    }
  }

  /**
   * Μικρή γλώσσα εκφράσεων:
   *  - var
   *  - ${var}
   *  - cond ? then : else
   *
   * όπου cond μπορεί να είναι:
   *  - left == placeholder
   *  - left == ''
   *  - left != ''
   *  - left == null
   *  - left != null
   *
   * Υποστηρίζει nested ternaries (με recursion):
   *   expr: "fdn != null ? 'FAULT_SYNC' :
   *          (objectId != null ? 'FAULT' : 'UNKNOWN')"
   *
   * Επιπλέον υποστηρίζει:
   *   expr: "UPPER(severity)"
   *   expr: "UPPER(someVar)"
   */
  private static String evalMiniExpr(String expr, TransformContext ctx, String placeholder) {
    if (expr == null) return null;

    String e = expr.trim();

    // Αφαίρεση ${ ... } αν υπάρχει
    if (e.startsWith("${") && e.endsWith("}")) {
      e = e.substring(2, e.length() - 1).trim();
    }

    // Αφαίρεση εξωτερικών παρενθέσεων αν είναι περιττές
    e = stripOuterParens(e);

    // --- NEW: UPPER(...) function support at top level ---
    if (isUpperCall(e)) {
      return evalUpperCall(e, ctx);
    }
    // --- END NEW ---

    // Αν δεν υπάρχει '?', είναι απλό var ή literal
    if (!e.contains("?")) {
      // Αν είναι σε quotes => literal
      if (isQuoted(e)) {
        return unquote(e);
      }
      // Αλλιώς θεώρησέ το όνομα μεταβλητής στο context
      Object v = ctx.get(e);
      return (v == null) ? null : v.toString();
    }

    // Υπάρχει τουλάχιστον ένα '?': ternary
    String[] parts = e.split("\\?", 2);          // split only once
    String cond = parts[0].trim();
    String[] arms = parts[1].split(":", 2);      // split only once
    String thenExpr = arms[0].trim();
    String elseExpr = arms[1].trim();

    boolean condTrue = evalCondition(cond, ctx, placeholder);

    String chosen = condTrue ? thenExpr : elseExpr;
    chosen = stripOuterParens(chosen.trim());

    // 1) Αν είναι literal σε quotes → επέστρεψέ το όπως είναι
    if (isQuoted(chosen)) {
      return unquote(chosen);
    }

    // 2) NEW: υποστήριξη UPPER(...) μέσα στα arms του ternary
    if (isUpperCall(chosen)) {
      return evalUpperCall(chosen, ctx);
    }

    // 3) Αν περιέχει πάλι '?', είναι nested ternary → recursion
    if (chosen.contains("?")) {
      return evalMiniExpr(chosen, ctx, placeholder);
    }

    // 4) Αλλιώς προσπάθησε να το διαβάσεις ως var από το context
    Object v = ctx.get(chosen);
    return (v == null) ? null : v.toString();
  }

  /**
   * Υποστήριξη συνθηκών:
   *   left == placeholder
   *   left == ''
   *   left != ''
   *   left == null
   *   left != null
   */
  private static boolean evalCondition(String cond, TransformContext ctx, String placeholder) {
    if (cond == null || cond.isBlank()) {
      return false;
    }

    String c = stripOuterParens(cond.trim());

    String op;
    if (c.contains("==")) {
      op = "==";
    } else if (c.contains("!=")) {
      op = "!=";
    } else {
      // fallback: "var" → true αν var != null
      Object lv = ctx.get(c);
      return lv != null;
    }

    String[] parts = c.split(op, 2);
    String left = parts[0].trim();
    String right = parts[1].trim();

    Object lv = ctx.get(left);
    String lvStr = (lv == null) ? null : lv.toString();

    String rv;
    if ("placeholder".equals(right)) {
      rv = placeholder;
    } else if ("null".equals(right)) {
      rv = null;
    } else {
      rv = unquote(right);
    }

    boolean eq = Objects.equals(lvStr, rv);
    return "==".equals(op) ? eq : !eq;
  }

  private static String stripOuterParens(String s) {
    if (s == null) return null;
    String r = s.trim();
    if (r.length() >= 2 && r.startsWith("(") && r.endsWith(")")) {
      // Απλοϊκό strip – καλύπτει την περίπτωσή μας
      return r.substring(1, r.length() - 1).trim();
    }
    return r;
  }

  private static boolean isQuoted(String s) {
    if (s == null || s.length() < 2) return false;
    char first = s.charAt(0);
    char last = s.charAt(s.length() - 1);
    return (first == '\'' && last == '\'') || (first == '"' && last == '"');
  }

  private static String unquote(String s) {
    if (s == null) return null;
    if (isQuoted(s)) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  // ─────────────────────────────────────────────
  // NEW: helpers για UPPER(...)
  // ─────────────────────────────────────────────

  private static boolean isUpperCall(String expr) {
    if (expr == null) return false;
    String e = expr.trim();
    // case-insensitive check για "UPPER(...)" μορφή
    return e.toUpperCase().startsWith("UPPER(") && e.endsWith(")");
  }

  private static String evalUpperCall(String expr, TransformContext ctx) {
    String e = expr.trim();
    // strip "UPPER(" + ")"
    String inner = e.substring(e.indexOf('(') + 1, e.length() - 1).trim();

    // Αν είναι literal "κάτι" → κάνε uppercase αυτό
    if (isQuoted(inner)) {
      return unquote(inner).toUpperCase();
    }

    // Αλλιώς θεωρείται όνομα μεταβλητής
    Object v = ctx.get(inner);
    return (v == null) ? null : v.toString().toUpperCase();
  }
}
