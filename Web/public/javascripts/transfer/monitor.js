// global
var gMainContent = false;

function initMonitor() {
  Ext.onReady(function() {
    renderPage();
  });
}

function renderPage() {
  gMainContent = createMonitorPanel();
  renderInMainViewport(gMainContent);
}

function createMonitorPanel() {
  var reqmon = createRequestMonitor();
  // create the main
  var mainContent = [reqmon];
  return mainContent;
}

function createRequestMonitor() {
  // create store
  var store = createRequestStore();
  // create columns
  var columns = [
    {header: "ReqID",
     dataIndex: "ReqID",
     sortable: true
    }
  ];

  // main
  var grid = new Ext.grid.GridPanel({
    store: store,
    columns: columns,
    region: 'center'
  });
  return grid;
}

function createRequestStore() {
  var data = [
    ["1"],
    ["2"],
    ["3"],
    ["4"]
  ];
  var store = new Ext.data.SimpleStore({
    fields: [
      "ReqID"
    ]
  });
  store.loadData(data);
  return store;
}
