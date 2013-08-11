// This file is used in monitor.js

function createNewRequest() {
  alert("createNewRequest");
  var form_id = Ext.id();

  var fields = new Ext.form.FieldSet({
    autoHeight: true,
    bodyStyle: 'padding: 5px',
    defaultType:'textfield',
    title: 'Create Transfer Request',
    monitorResize: true,
    items: [{
      anchor: '-5',
      fieldLabel: "Dataset",
      name: 'dataset',
      allowBlank: false,
    }, {
      anchor: '-5',
      fieldLabel: "SRC SE",
      name: 'src_se',
      allowBlank: false,
    }, {
      anchor: '-5',
      fieldLabel: "DST SE",
      name: 'dst_se',
      allowBlank: false,
    }]
  });

  // TODO 
  // Check the user
  // Construct the URL
  var group = gPageDescription.userData.group;
  var setup = gPageDescription.selectedSetup;
  var url = 'https://' + location.host + '/DIRAC/' + setup + '/' + group;
  url = url + '/transfer/reqmgr/create';

  // create submit button
  var submit = new Ext.Button({
    text: "create",
    minWidth: '70',
    handler: function() {
      alert("Create A Request");
    }
  });

  var panel = new Ext.FormPanel({
    layout: "fit",
    labelWidth: 100,
    items: [fields],
    buttons: [submit],
    id: form_id,
    monitorResize: true,
    method: "POST",
    url: url
  });

  var win = new Ext.Window({
    closable: true,
    constrain: true,
    constrainHeader: true,
    layout: "fit",
    title: "Create New Transfer Request",
    width: 320,
    height: 240,
    items: [panel],
    monitorResize:true,
  });

  win.show();
}
