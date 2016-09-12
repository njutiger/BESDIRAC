var gPlotsList = []; //Valid list of plots
var gTypeName = ""; //Type name
var gMainPanel = false;

function initAcctPlots( plotsList, selectionData ){
  gPlotsList = plotsList;
  gTypeName = "DataTransfer";
  Ext.onReady(function(){
    renderPage( plotsList, selectionData );
  });
}

function renderPage( plotsList, selectionData ){
  initPlotPage( "Data Transfer plot generation" );
  // part 1
  appendToLeftPanel( createComboBox( "plotName", "Plot to generate", "Select a plot", plotsList ) );

  // part 2
  var orderKeys = [];
  for( key in selectionData )
  {
  	orderKeys.push( [ key, key ] );
  }
  appendToLeftPanel( createComboBox( "grouping", "Group by", "Select grouping", orderKeys ) );

  // part 3
  appendTimeSelectorToLeftPanel();
  // part 4
  var selWidgets = []

  selWidgets.push( createMultiselect( "User", "User", selectionData.User ) );
  selWidgets.push( createMultiselect( "Source", "Source site", selectionData.Source ) );
  selWidgets.push( createMultiselect( "Destination", "Destination site", selectionData.Destination ) );
  selWidgets.push( createMultiselect( "Protocol", "Protocol", selectionData.Protocol ) );
  selWidgets.push( createMultiselect( "FinalStatus", "Final transfer status", selectionData.FinalStatus ) );
  selWidgets.push( createHidden( "typeName", "DataTransfer" ) );
  appendToLeftPanel( createPanel( "Selection conditions", selWidgets ) );

  // part 5
  appendAdvancedSettingsWidget();
  // final
  renderPlotPage();
}
