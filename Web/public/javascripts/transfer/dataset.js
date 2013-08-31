// global

var gMainContent = false;

function initDatasets() {
  Ext.onReady(function() {
    gMainContent = [createDatasetsPanel()];
    renderInMainViewport(gMainContent);
  });
}

function createDatasetsPanel() {
  // show the datasets

  // create store
  var store = createDatasetsStore();
  store.load({ params: { start: 0, limit: 20} });
  // create columns
  var columns = [
    {header: "Dataset ID",
     dataIndex: "id",
     sortable: true
    },
    {header: "User Name",
     dataIndex: "username",
     sortable: true
    },
    {header: "Dataset",
     dataIndex: "name",
     sortable: true
    }
  ]
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
    id: 'gMainDatasetsList',
    store: store,
    sm: new Ext.grid.RowSelectionModel({singleSelect: true}),
    columns: columns,
    region: 'center',
    tbar: topbar,
    bbar: bottombar
  });
  return grid;
}

function createTopBar() {
  var topbar = [
    {handler: function(wiget, event) {
      var grid = Ext.getCmp("gMainDatasetsList");
      grid.getStore().reload();
    },
    text: "Refresh"},
    "-",
    {handler: function(wiget, event) {
      // TODO
      createFileListWindow();
    },
    text: "Show Files' State"},
  ];
  return topbar;
}

function createDatasetsStore() {
  var reader = new Ext.data.JsonReader({
    root: 'data',
    totalProperty: 'num',
    id: 'id',
    fields:['id',
            'username',
            'name'
            ]
  });
  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/dataset/datasets';
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

function createFileListWindow() {
  var fl_id = Ext.id()
  // TODO
  // Get the Request ID
  var grid = Ext.getCmp("gMainDatasetsList");
  var selected = grid.getSelections();
  console.info('selected ', selected);

  if(selected.length != 1) {
    // Make sure we just select one row 
    return;
  } 
  dataset = selected[0].data.name;

  alert(dataset);

  // Get the files in the dataset
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
    {header:'dataset_id',
     dataIndex:'dataset_id',
     sortable: true
    }
  ];

  var grid = new Ext.grid.GridPanel({
    id: fl_id,
    store: store,
    columns: columns,
    layout: "fit",
    height:400,
    autoHeight: true,
    region: 'center',
    selModel: new Ext.grid.RowSelectionModel({singleSelect : true}),
    //tbar: topbar,
    viewConfig: {
      forceFit: true
    }
  });
  grid.on({
    render: {
      //scope: this,
      fn: function() {
        //alert("Load Data Req ID: " + req_id);
        grid.getStore().load({params:{dataset: dataset}});
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
      {name:"dataset_id"}
    ]);

  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/dataset/ls';
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
