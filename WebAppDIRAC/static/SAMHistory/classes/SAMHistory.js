Ext.define('DIRAC.SAMHistory.classes.SAMHistory', {
	extend : 'Ext.dirac.core.Module',

	initComponent : function() {
		var me = this;

		me.launcher.title = 'SAM History';
		me.launcher.maximized = false;
		var oDimensions = GLOBAL.APP.MAIN_VIEW.getViewMainDimensions();
		me.launcher.width = oDimensions[0];
		me.launcher.height = oDimensions[1];
		me.launcher.x = 0;
		me.launcher.y = 0;

		Ext.apply(me, {
					layout : 'border',
					bodyBorder : false,
					defaults : {
						collapsible : true,
						split : true
					}
				});

		me.callParent(arguments);
	},

	buildUI : function() {
		var me = this;

		var selectors = {
			site : 'Site',
			type : 'Site Type',
			mask : 'Mask Status',
			vo : 'VO'
		};

		var map = [['site', 'site'], ['type', 'type'], ['mask', 'mask'],
				['vo', 'vo']]

		me.leftPanel = Ext.create('Ext.dirac.utils.DiracBaseSelector', {
					scope : me,
					cmbSelectors : selectors,
					hasTimeSearchPanel : true,
					datamap : map,
					url : me.applicationName + '/getSelectionData'
				});
		me.leftPanel.oprLoadGridData = me.oprLoadGridData;

		var pagingToolbar = Ext.create('Ext.dirac.utils.DiracPagingToolbar', {
					store : me.datastore,
					scope : me,
					value : 25
				});

		me.grid = Ext.create('Ext.dirac.utils.DiracGridPanel', {
					columnLines : true,
					pagingToolbar : pagingToolbar,
					scope : me
				});

		me.leftPanel.setGrid(me.grid);
		me.add([me.leftPanel, me.grid]);
	},

	oprLoadGridData : function() {
		var me = this;
		oParams = me.getSelectionData();

		Ext.Ajax.request({
			url : GLOBAL.BASE_URL + 'SAMHistory/getMainData',
			params : oParams,
			method : 'POST',
			scope : me,
			success : function(response) {
				var me = this;

				var respText = Ext.decode(response.responseText);

				if (respText.success == 'false') {
					var errorMsg = 'Service Error : ' + respText.error;
					Ext.MessageBox.alert('error', errorMsg);
				} else {
					var datastore = Ext.create('Ext.data.JsonStore', {
								data : respText.data,
								fields : respText.dataFields
							});

					columns = respText.columns;
					for (var i in columns) {
						if (columns[i].text != 'Site') {
							columns[i].renderer = me.up('container').__renderSAMStatus;
						}
					}
					me.grid.reconfigure(datastore, columns);
				}
			},
			failure : function(response) {
				var errorMsg = 'HTTP Error(' + response.status + ') : '
						+ response.statusText;
				Ext.MessageBox.alert('error', errorMsg);
			}
		});
	},

	__renderSAMStatus : function(v) {
		switch (v) {
			case 'OK' :
				return '<font color="green">' + v + '</font>';
			case 'Warn' :
				return '<font color="yellow">' + v + '</font>';
			case 'Bad' :
				return '<font color="red">' + v + '</font>';
			case 'Unknown' :
				return '<font color="blue">' + v + '</font>';
			default :
				return '<font color="black">' + v + '</font>';
		}
	}

});