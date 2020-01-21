artifacts builderVersion: "1.1", {

  group "com.sap.bds.ats-altiscale", {

    artifact "oozie", {
      file "$gendir/src/oozieclientrpmbuild/oozie-client-artifact/alti-oozie-client-${buildVersion}.rpm", classifier:"client"
      file "$gendir/src/oozieserverrpmbuild/oozie-server-artifact/alti-oozie-server-${buildVersion}.rpm", classifier:"server"
    }
  }
}
