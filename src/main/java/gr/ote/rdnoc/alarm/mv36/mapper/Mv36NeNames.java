package gr.ote.rdnoc.alarm.mv36.mapper;

/**
 * UnifiedEvent neName for MV36 (Mobile, Fixed A/B/C): the NE name plus the ".N" suffix of the
 * NE unique name, e.g. mv36NeName "D230-23_Kilkis_OMS1684" + mv36NeUniqueName "KILKIS.3"
 * -> "D230-23_Kilkis_OMS1684.3".
 *
 * Used by both the trap path (FAULT / CLEAR) and the SNMP sync path (FAULT_SYNC) so the two
 * always produce the same neName.
 */
public final class Mv36NeNames {

  private Mv36NeNames() {
  }

  /**
   * @return neName with the unique-name suffix, or {@code null} when {@code neName} is blank
   *         (callers keep their own fallback). Without a usable suffix, neName is returned as is.
   */
  public static String withUniqueNameSuffix(String neName, String uniqueName) {
    String name = clean(neName);
    if (name == null) {
      return null;
    }

    String unique = clean(uniqueName);
    if (unique == null) {
      return name;
    }

    int dot = unique.lastIndexOf('.');
    if (dot < 0 || dot == unique.length() - 1) {
      return name;
    }

    String suffix = unique.substring(dot);
    return name.endsWith(suffix) ? name : name + suffix;
  }

  private static String clean(String v) {
    if (v == null) {
      return null;
    }
    String s = v.trim();
    return s.isEmpty() || "--".equals(s) ? null : s;
  }
}
