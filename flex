private void applyHighlightLogic() {
    // Get values for cust_line_profile and mtz_adobe
    String custLineVal = children.stream()
            .filter(c -> "cust_line_profile".equals(c.getSourceTable()))
            .map(ParentData::getSmpDownPricingPlans)
            .findFirst()
            .orElse(null);

    String adobeVal = children.stream()
            .filter(c -> "mtz_adobe".equals(c.getSourceTable()))
            .map(ParentData::getSmpDownPricingPlans)
            .findFirst()
            .orElse(null);

    if (custLineVal != null && adobeVal != null) {
        if (custLineVal.equals(adobeVal)) {
            this.highlight = "green";   // ✅ matching
        } else {
            this.highlight = "red";     // ❌ not matching
        }
    } else {
        this.highlight = "no_highlight"; // default when either is missing
    }
}
