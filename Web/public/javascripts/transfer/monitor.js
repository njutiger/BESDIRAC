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
     dataIndex: "id",
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
  var reader = new Ext.data.JsonReader({
    root: 'data',
    totalProperty: 'num',
    id: 'id',
    fields:['id',
            'username',
            'dataset',
            'srcSE',
            'dstSE',
            'submit_time',
            'status',
            ]
  });
  var setup = gPageDescription.selectedSetup;
  // confirm the user is OK
  //if (!gPageDescription.userData && !gPageDescription.userData.group) {
  //  return;
  //}
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/monitor/reqs';
  var store = new Ext.data.Store({
    reader: reader,
    proxy: new Ext.data.HttpProxy({
              url: url,
              method: 'POST'
            }),
    autoLoad: true
  });
  store.load();
  return store;
}
