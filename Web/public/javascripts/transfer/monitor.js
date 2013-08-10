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
    id: 'gMainRequestsList',
    store: store,
    sm: new Ext.grid.RowSelectionModel({singleSelect: true}),
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
  var topbar = [
    {handler: function(wiget, event) {
      createFileListWindow();
    },
    text: "Show Files' State"},
  ];
  return topbar;
}

function createFileListWindow() {
  var fl_id = Ext.id()
  // TODO
  // Get the Request ID
  var grid = Ext.getCmp("gMainRequestsList");
  var selected = grid.getSelections();
  if(selected.length != 1) {
    // set not exist
    req_id = -1;
  } else {
    req_id = selected[0].id;
  }
  var store = createFileListStore();
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
    },
    {header:'Error',
     dataIndex:'error',
     renderer: function(value, metadata, record, rowIndex, colIndex, store) {
      console.log(record);
      if (record.json && record.json.error && record.json.error.length>0) {
        return '<a onclick="hello('
               +"'"+fl_id+"'"
               +","
               +"'"+rowIndex+"'"
               +')">Error</a>'
      } else {
        return 'OK'
      } 
     },
     sortable: false
    }
  ];

  // top bar
  var topbar = createFileListTopBar(fl_id);

  var grid = new Ext.grid.GridPanel({
    id: fl_id,
    store: store,
    columns: columns,
    layout: "fit",
    height:400,
    autoHeight: true,
    region: 'center',
    selModel: new Ext.grid.RowSelectionModel({singleSelect : true}),
    tbar: topbar,
    viewConfig: {
      forceFit: true
    }
  });
  grid.on({
    render: {
      //scope: this,
      fn: function() {
        //alert("Load Data Req ID: " + req_id);
        grid.getStore().load({params:{req_id: req_id}});
      }
    }
  });

  var win = new Ext.Window({
    closable: true,
    width: 600,
    height: 400,
    border: true,
    autoHeight: true,
    title: "Files Monitor",
    items: [grid],
    layout: "fit",
  });

  win.show();
  //grid.reconfigure(store);
  return win;
}

function createFileListStore() {
  var reader = new Ext.data.JsonReader({
    root: 'data',
    totalProperty: 'num',
    id: 'id',
    
  },[
      {name:"id"},
      {name:"LFN"},
      {name:"trans_req_id"},
      {name:"start_time"},
      {name:"finish_time"},
      {name:"status"},
      {name:"error"}
    ]);
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
            }),
    autoLoad: false,
    autoSync: true
  });
  store.on({
      'load':{
          fn: function(store, records, options){
              //store is loaded, now you can work with it's records, etc.
              console.info('store load, arguments:', arguments);
              console.info('Store count = ', store.getCount());
          },
          scope:this
      },
      'loadexception':{
          //consult the API for the proxy used for the actual arguments
          fn: function(obj, options, response, e){
              console.info('store loadexception, arguments:', arguments);
              console.info('error = ', e);
          },
          scope:this
      }
  });
  return store;
}

function createFileListTopBar(fl_id) {
  var topbar = [
    // 1. Refresh
    {
      handler: function(w,t) {
        var grid = Ext.getCmp(fl_id);
        grid.getStore().reload();
      },
      text: "Refresh"
    },
    // 2. Get Error Info
    {
      handler: function(w,t) {
        var grid = Ext.getCmp(fl_id);
        getErrorInfo(grid);
      },
      text: "Get Error"
    }
  ];
  return topbar;
}

function getErrorInfo(grid) {
  var selected = grid.getSelections();
  if (selected.length != 1) {
    return;
  }
  // Get the error info
  var error_info = selected[0].json.error;
  // Create a panel for the Error Info.
  alert(error_info);
}

function hello(grid_id, row_index) {
  alert(grid_id);
  alert(row_index);

  var grid = Ext.getCmp(grid_id);
  getErrorInfo(grid);
}
