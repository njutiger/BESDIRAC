# -*- coding: utf-8 -*-
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<%inherit file="/diracPage.mako" />

<%def name="head_tags()">
</%def>

<%def name="body()">
<script type="text/javascript">
  var html = "<p>Hello</p>";
  var mainContent = new Ext.Panel({html:html, region:'center'});
  renderInMainViewport([mainContent]);
</script>
</%def>
