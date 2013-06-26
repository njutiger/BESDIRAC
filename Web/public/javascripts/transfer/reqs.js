var gTransferRequest = false;

function initReqs() {
  Ext.onReady(function() {
    gTransferRequest = createTransferRequest();
    renderInMainViewport([gTransferRequest]);
  });
}

function createTransferRequest() {
  // TODO: make sure the labels are all right
  var reader = new Ext.data.JsonReader({
    root: 'reqlist',
    totalProperty: 'numofreq',
    id: 'reqid',
    fields: ['reqid'],
  });
  var store = new Ext.data.Store({
    reader: reader,
    proxy: new Ext.data.HttpProxy({
      url: create_url("/transfer/reqs/getReqs")
    }),
    autoLoad: true,
    sortInfo: {field: 'reqid', direction: 'DESC'},
  });
  columns = [
    {"header": "Req ID",
     dataIndex: "reqid",
     sortable: true,
    },
  ];
  // paging
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
    bbar: bottombar
  });
  return grid;
}

// helper
function create_url(url) {
  var setup = gPageDescription.selectedSetup;
  var group = gPageDescription.userData.group;
  var new_url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  new_url = new_url + url;
  return new_url;
}
