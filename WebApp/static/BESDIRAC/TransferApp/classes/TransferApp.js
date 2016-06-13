Ext.define('BESDIRAC.TransferApp.classes.TransferApp', {
    extend : 'Ext.dirac.core.Module',
    requires : ['Ext.util.*', 
                'Ext.panel.Panel', 
                "Ext.form.field.Text", 
                "Ext.button.Button", 
                "Ext.menu.Menu", 
                "Ext.form.field.ComboBox", 
                "Ext.layout.*", 
                "Ext.form.field.Date", 
                "Ext.form.field.TextArea", 
                "Ext.form.field.Checkbox", 
                "Ext.form.FieldSet",
                "Ext.dirac.utils.DiracMultiSelect", 
                "Ext.toolbar.Toolbar", 
                "Ext.data.Record", 
                'Ext.Array', 
                'Ext.data.TreeStore', 
                'Ext.layout.container.Accordion',
                "Ext.ux.form.MultiSelect"
               ],
    // = initComponent =
    initComponent: function() {
        var me = this;
    
        //setting the title of the application
        me.launcher.title = "Transfer Application";
        //setting the maximized state
        me.launcher.maximized = true;
    
        Ext.apply(me, {
              layout : 'border',
              bodyBorder : false,
              defaults : {
                collapsible : true,
                split : true
              },
              items : [],
              header : false
            });

        me.callParent(arguments);


    },

    // = buildUI =
    buildUI : function() {
        var me = this;

        // top toolbar -> me.top_toolbar
        me.build_toolbar();
        // main panel -> me.main_panel
        me.build_mainpanel();

        // + requests
        me.build_panel_requests();
        // + datasets
        me.build_panel_datasets();

        me.add([me.main_panel]);
        console.log("hello");
    }, 

    build_toolbar : function() {
        var me = this;
        me.top_toolbar = Ext.create('Ext.toolbar.Toolbar', {
              dock : 'top',
              border : false,
              layout : {
                pack : 'center'
              },
              items : [{
                   xtype: "button",
                   text: "Requests",
                   handler : function() {
                      me.main_panel.getLayout().setActiveItem(0);
                   },
                   toggleGroup : me.id + "-ids-submodule",
                   allowDepress : false
                }, {
                   xtype: "button",
                   text: "Datasets",
                   handler : function() {
                      me.main_panel.getLayout().setActiveItem(1);
                   },
                   toggleGroup : me.id + "-ids-submodule",
                   allowDepress : false
              }]
              });
        me.top_toolbar.items.getAt(0).toggle();
    },

    build_mainpanel : function() {
        var me = this;
        me.main_panel = new Ext.create('Ext.panel.Panel', {
              floatable : false,
              layout : 'card',
              region : "center",
              header : false,
              border : false,
              dockedItems : [me.top_toolbar]
            });

    }, 

    build_panel_requests : function() {
        var me = this;
        // TODO
        me.panel_requests = new Ext.create('Ext.form.field.TextArea', {
            fieldLabel : "Value",
            labelAlign : "top",
            flex : 1
        });

        me.main_panel.add([me.panel_requests]);
    },

    build_panel_datasets : function() {
        var me = this;
        // TODO
        // + dataset list
        me.panel_datasets_list = new Ext.create('Ext.form.field.TextArea', {
            fieldLabel : "Value",
            labelAlign : "top",
            columnWidth: .25,
        });
        // + file list in dataset
        me.panel_files_in_dataset = new Ext.create('Ext.form.field.TextArea', {
            fieldLabel : "Value",
            labelAlign : "top",
            columnWidth: .75,
        });

        me.panel_datasets = new Ext.create('Ext.panel.Panel', {
              floatable : false,
              // layout : 'accordion',
              layout : 'column',
              header : false,
              border : false,
              items : [me.panel_datasets_list, me.panel_files_in_dataset]
            });

        me.main_panel.add([me.panel_datasets]);
    },
});
