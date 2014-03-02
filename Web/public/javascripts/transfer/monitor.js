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
  store.load({ params: { start: 0, limit: 20} });
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
    {header: "Protocol",
     dataIndex: "protocol",
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

  // paging
  var bottombar = new Ext.PagingToolbar({
    pageSize: 20,
    store: store,
    displayInfo: true,
    displayMsg: 'Displaying {0} - {1} of {2}'
  });

  // main
  var grid = new Ext.grid.GridPanel({
    id: 'gMainRequestsList',
    store: store,
    sm: new Ext.grid.RowSelectionModel({singleSelect: true}),
    columns: columns,
    region: 'center',
    tbar: topbar,
    bbar: bottombar
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
            'protocol',
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
    autoLoad: false,
    autoSync: true
  });
  store.on({
      'load':{
          fn: function(store, records, options){
              //store is loaded, now you can work with it's records, etc.
              console.info('req store load, arguments:', arguments);
              console.info('req Store count = ', store.getCount());
          },
          scope:this
      },
      'loadexception':{
          //consult the API for the proxy used for the actual arguments
          fn: function(obj, options, response, e){
              console.info('req store loadexception, arguments:', arguments);
              console.info('req error = ', e);
          },
          scope:this
      }
  });
  return store;
}

function createTopBar() {
  var topbar = [
    {handler: function(wiget, event) {
      var grid = Ext.getCmp("gMainRequestsList");
      grid.getStore().reload();
    },
    text: "Refresh"},
    "-",
    {handler: function(wiget, event) {
      createFileListWindow();
    },
    text: "Show Files' State"},
    "-",
    {handler: function(w, e) {
      createNewRequest();
    },
    text: "Create New Request" },
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
     sortable: true
    }
  ];

  // top bar
  var topbar = createFileListTopBar(fl_id);

  var grid = new Ext.grid.GridPanel({
    id: fl_id,
    store: store,
    columns: columns,
    layout: "fit",
    width: 600,
    height:400,
    //autoHeight: true,
    region: 'center',
    selModel: new Ext.grid.RowSelectionModel({singleSelect : true}),
    tbar: topbar,
    viewConfig: {
      forceFit: true
    },
    autoScroll: true,
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
    //autoHeight: true,
    title: "Files Monitor",
    items: [grid],
    layout: "fit",
    autoScroll: true,
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
    },
    // 3. Kill the transfer file
    {
      handler: function(w,t) {
        var grid = Ext.getCmp(fl_id);
        var selected = grid.getSelections();
        if(selected.length != 1) {
          // set not exist
          file_id = -1;
        } else {
          file_id = selected[0].id;
        }
        // RPC Call
        fileTransferKill(file_id, grid);
      },
      text: "Kill"
    },
    // 4. Retransfer 
    {
      handler: function(w,t) {
        var grid = Ext.getCmp(fl_id);
        var selected = grid.getSelections();
        if(selected.length != 1) {
          // set not exist
          file_id = -1;
        } else {
          file_id = selected[0].id;
        }
        // RPC Call
        fileTransferRetransfer(file_id, grid);
      },
      text: "Retransfer"
    },
  ];
  return topbar;
}

function fileTransferKill(fileid, grid) {
  alert("Will Kill "+fileid);

  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/reqmgr/delete';

  Ext.Ajax.request({
    method:'POST',
    params:{id:fileid},
    url: url,
    success: function(response){
      grid.getStore().reload();
    },
    failure: function(response){
      grid.getStore().reload();
    }
  });
}

function fileTransferRetransfer(fileid, grid) {
  alert("Will Retransfer "+fileid);

  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/reqmgr/retransfer';

  Ext.Ajax.request({
    method:'POST',
    params:{id:fileid},
    url: url,
    success: function(response){
      grid.getStore().reload();
    },
    failure: function(response){
      grid.getStore().reload();
    }
  });
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
  var win = new Ext.Window({
    closable: true,
    width: 600,
    height: 400,
    //autoHeight: true,
    autoScroll: true,
    title: "Error Info",
    layout: "fit",
    html: "<pre>"+error_info+"</pre>"
  });

  win.show();
}

function hello(grid_id, row_index) {
  alert(grid_id);
  alert(row_index);

  var grid = Ext.getCmp(grid_id);
  getErrorInfo(grid);
}
