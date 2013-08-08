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
    },
    {header: "User Name",
     dataIndex: "username",
     sortable: true
    },
    {header: "Dataset",
     dataIndex: "dataset",
     sortable: true
    },
    {header: "src SE",
     dataIndex: "srcSE",
     sortable: true
    },
    {header: "dst SE",
     dataIndex: "dstSE",
     sortable: true
    },
    {header: "submit time",
     dataIndex: "submit_time",
     sortable: true
    },
    {header: "status",
     dataIndex: "status",
     sortable: true
    }
  ];

  // top bar
  var topbar = createTopBar();

  // main
  var grid = new Ext.grid.GridPanel({
    store: store,
    columns: columns,
    region: 'center',
    tbar: topbar
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

function createTopBar() {
  topbar = [
    {handler: function(wiget, event) {
      alert("Hello") 
    },
    text: "Show Files' State"},
  ];
  return topbar;
}
