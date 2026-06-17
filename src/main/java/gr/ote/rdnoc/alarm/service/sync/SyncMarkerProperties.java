package gr.ote.rdnoc.alarm.service.sync;

import org.springframework.boot.context.properties.ConfigurationProperties;

import gr.ote.atlas.events.enums.EMSDomain;
import gr.ote.atlas.events.enums.EMSId;
import gr.ote.atlas.events.enums.EMSVendorID;

@ConfigurationProperties(prefix = "app.sync.marker")
public class SyncMarkerProperties {

  /**
   * Defaults preserve old NSP/ATNOI behavior if the config is missing.
   */
  private EMSId sourceEms = EMSId.NSP_ATNOI;
  private EMSVendorID emsVendorId = EMSVendorID.NSP;
  private EMSDomain emsDomain = EMSDomain.UNKNOWN;

  public EMSId getSourceEms() {
    return sourceEms;
  }

  public void setSourceEms(EMSId sourceEms) {
    this.sourceEms = sourceEms;
  }

  public EMSVendorID getEmsVendorId() {
    return emsVendorId;
  }

  public void setEmsVendorId(EMSVendorID emsVendorId) {
    this.emsVendorId = emsVendorId;
  }

  public EMSDomain getEmsDomain() {
    return emsDomain;
  }

  public void setEmsDomain(EMSDomain emsDomain) {
    this.emsDomain = emsDomain;
  }
}