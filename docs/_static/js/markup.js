$(document).ready(function () {
  // dim the `None` parameter a bit
  $('.sig-param .n .pre:contains("None")').addClass("hb-param-none");
  $('.sig-param:has(.default_value:contains("None"))').addClass(
    "hb-param-none-default",
  );

  // hide headers for nodes we marked with `hide-header`
  $('.sig-object:has(.sig-param:contains("hide-header"))').addClass(
    "hb-display-none",
  );
});
