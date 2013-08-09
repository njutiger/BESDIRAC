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
      createFileListWindow();
    },
    text: "Show Files' State"},
  ];
  return topbar;
}

function createFileListWindow() {
  // TODO
  var req_id = 0;
  var store = createFileListStore(req_id);
  var columns = [
    {header:'id',
     dataIndex:'id',
     sortable: true
    },
    {header:'LFN',
     dataIndex:'LFN',
     sortable: true
    },
    {header:'Start Time',
     dataIndex:'start_time',
     sortable: true
    },
    {header:'Finish Time',
     dataIndex:'finish_time',
     sortable: true
    },
    {header:'Status',
     dataIndex:'status',
     sortable: true
    }
  ];
  var grid = new Ext.grid.GridPanel({
    store: store,
    columns: columns,
    region: 'center'  
  });
  var win = new Ext.Window({
    closable: true,
    width: 600,
    height: 400,
    border: true,
    title: "Files Monitor",
    items: [grid]
  });

  win.show();
  return win;
}

function createFileListStore(req_id) {
  var reader = new Ext.data.JsonReader({
    root: 'data',
    totalProperty: 'num',
    id: 'id',
    fileds: [
      "id",
      "LFN",
      "trans_req_id",
      "start_time",
      "finish_time",
      "status",
      "error"
    ]
  });
  // TODO
  var setup = gPageDescription.selectedSetup;
  // confirm the user is OK
  //if (!gPageDescription.userData && !gPageDescription.userData.group) {
  //  return;
  //}
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/monitor/req';
  var store = new Ext.data.Store({
    reader: reader,
    proxy: new Ext.data.HttpProxy({
              url: url,
              method: 'POST',
              params: {
                req_id: req_id
              }
            }),
    autoLoad: true
  });
  store.load();
  return store;
}
