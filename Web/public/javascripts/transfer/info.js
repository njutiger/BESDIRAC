var gMainContent = false;
var gRequestList = false;
var gFileList = false;
var gSelModel = false;

function initInfo() {
  Ext.onReady(function() {
    renderPage();
  });
}

function renderPage() {
  gMainContent = createInfoPanel();
  renderInMainViewport(gMainContent);
}

function createInfoPanel() {
  var grid = createRequestPanel();
  // Another grid
  var grid2 = createFileListPanel();
  // End
  //var html = "<p>Hello</p>";
  var mainContent = [grid, grid2];
  gRequestList = grid;
  gFileList = grid2;
  return mainContent;
}

// Create File List Panel
function createFileListPanel() {
  // create Reader
  var reader = new Ext.data.JsonReader({
    root: 'data',
    totalProperty: 'num',
    //id: 'author',
    fields: ["author", "detail"]
  });
  // create Store
  var store = new Ext.data.Store({
    reader: reader,
    proxy: new Ext.data.HttpProxy({
      url:''
    }),
    autoLoad: false,
    sortInfo: {field: 'author', direction: 'DESC'},
  });
  // create columns
  var columns = [
    {"header": "Name",
      dataIndex: "author",
      sortable: true
    },
    {"header": "Detail",
      dataIndex: "detail",
      sortable: true
    },
  ]
  // paging
  var bottombar = new Ext.PagingToolbar({
    pageSize: 50,
    store: store,
    displayInfo: true,
    displayMsg: 'Displaying {0} - {1} of {2}'
  });
  var grid2 = new Ext.grid.GridPanel({
    store: store,
    columns: columns,
    region: 'east',
    width: '50%',
    bbar: bottombar
  });
  return grid2;
}

// Create Request Panel
function createRequestPanel() {
  // create Reader
  var reader = new Ext.data.JsonReader({
    root: 'functions',
    totalProperty: 'numRecords',
    id: 'FuncName',
    fields: ["FuncName", "ScriptName"]
  });
  // create Store
  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/info/getInfoList';
  var store = new Ext.data.Store({
    reader: reader,
    url: url,
    autoLoad: true,
    sortInfo: {field: 'FuncName', direction: 'DESC'},
    listeners: {
      beforeload: cbStoreBeforeLoad
    }
  });
  // Create the panel
  var title = "Transfer System Info"
  var columns = [
    {header: '', 
        name: 'checkBox', 
        id: 'checkBox', 
        dataIndex: 'FuncCheckBox',
        renderer: function(value, metadata, record, rowIndex, colIndex, store) {
          var url = gURLRoot + '/' + gPageDescription.selectedSetup + '/' + record.id
          return '<a id="' + record.id + '" href="' + url + '">View</a>';
        },
        hideable: false,
        fixed: true,
        menuDisabled: true
    },
    {header: 'Function', sortable: true, dataIndex: 'FuncName'},
    {header: 'Script', sortable: true, dataIndex: 'ScriptName'}
  ];

  var topbar = [
    { handler: function(wiget, event) {
        showInfo(gRequestList);
      },
      text: 'View Selected',
      width: 150,
      tooltip: 'Click to view selected row'
    },
  ];

  var bottombar = new Ext.PagingToolbar({
    pageSize: 50,
    store: store,
    displayInfo: true,
    displayMsg: 'Displaying {0} - {1} of {2}'
  });

  var grid = new Ext.grid.GridPanel({
    store: store,
    columns: columns,
    region: 'center',
    tbar: topbar,
    bbar: bottombar,
    sm: new Ext.grid.RowSelectionModel({singleSelect:true}),
    width: '50%'
  });
  grid.getSelectionModel().on('rowselect', function(sm, rowIdx, r) {
    showInfo(gRequestList)
  }
  );

  return grid;
}

// helper functions
function cbStoreBeforeLoad(store, params)
{
  var sortState = store.getSortState();
  var bb = gRequestList.getBottomToolbar();
  store.baseParams = {
    'sortField': sortState.field,
    'sortDirection': sortState.direction,
    'limit': bb.pageSize,
  };
}

function showInfo( grid ) {
  var selModel = grid.getSelectionModel();
  var rec = selModel.getSelected();

  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/info/getDetailList?funcname=' + rec.get("FuncName");

  gFileList.store.proxy = new Ext.data.HttpProxy({
    url: url
  });
  gFileList.store.load();
}
