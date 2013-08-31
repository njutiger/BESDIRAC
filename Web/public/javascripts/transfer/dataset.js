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
