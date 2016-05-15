Ext.define('BESDIRAC.SiteStatusMonitor.classes.SiteStatusMonitor', {
	extend : 'Ext.dirac.core.Module',
	requires : [],

	dataFields : [{
				name : 'Site'
			}, {
				name : 'SiteType'
			}, {
				name : 'Icon',
				mapping : 'MaskStatus'
			}, {
				name : 'MaskStatus'
			}, {
				name : 'VO'
			}, {
				name : 'CEStatus'
			}, {
				name : 'SEStatus'
			}, {
				name : 'SEOccupied'
			}, {
				name : 'Running'
			}, {
				name : 'Waiting'
			}, {
				name : 'Done'
			}, {
				name : 'Failed'
			}, {
				name : 'Completed'
			}, {
				name : 'Stalled'
			}, {
				name : 'Efficiency'
			}],

	initComponent : function() {
		var me = this;

		me.launcher.title = 'Site Status Monitor';
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
					hasTimeSearchPanel : false,
					datamap : map,
					url : me.applicationName + '/getSelectionData'
				});

		var oColumns = {
			'Site' : {
				'dataIndex' : 'Site',
				'properties' : {
					width : 120
				}
			},
			'SiteType' : {
				'dataIndex' : 'SiteType',
				'properties' : {
					width : 80
				}
			},
			'None2' : {
				'dataIndex' : 'Icon',
				'properties' : {
					width : 26,
					sortable : false,
					hideable : false,
					fixed : true,
					menuDisabled : true
				},
				'renderFunction' : 'rendererStatus'
			},
			'MaskStatus' : {
				'dataIndex' : 'MaskStatus',
				'properties' : {
					width : 80
				}
			},
			'VO' : {
				'dataIndex' : 'VO',
				'properties' : {
					width : 50
				}
			},
			'CE-Test' : {
				'dataIndex' : 'CEStatus',
				'properties' : {
					width : 60
				},
				'renderer' : me.__renderSAMStatus
			},
			'SE-Test' : {
				'dataIndex' : 'SEStatus',
				'properties' : {
					width : 60
				},
				'renderer' : me.__renderSAMStatus
			},
			'SE Occupied' : {
				'dataIndex' : 'SEOccupied',
				'properties' : {
					width : 80
				},
				'renderer' : me.__renderSAMStatus
			},
			'R' : {
				'dataIndex' : 'Running',
				'properties' : {
					width : 20
				}
			},
			'W' : {
				'dataIndex' : 'Waiting',
				'properties' : {
					width : 20
				}
			},
			'D' : {
				'dataIndex' : 'Done',
				'properties' : {
					width : 20
				}
			},
			'F' : {
				'dataIndex' : 'Failed',
				'properties' : {
					width : 20
				}
			},
			'C' : {
				'dataIndex' : 'Completed',
				'properties' : {
					width : 20
				}
			},
			'S' : {
				'dataIndex' : 'Stalled',
				'properties' : {
					width : 20
				}
			},
			'Efficiency' : {
				'dataIndex' : 'Efficiency',
				'properties' : {
					width : 60
				}
			}
		};

		var oproxy = Ext.create('Ext.dirac.utils.DiracAjaxProxy', {
					url : GLOBAL.BASE_URL + me.applicationName + '/getMainData'
				});

		me.datastore = Ext.create('Ext.dirac.utils.DiracJsonStore', {
					proxy : oproxy,
					fields : me.dataFields,
					scope : me,
					remoteSort : false
				});

		var pagingToolbar = Ext.create('Ext.dirac.utils.DiracPagingToolbar', {
					store : me.datastore,
					scope : me,
					value : 25
				});

		var menuitems = {
			'Visible' : [{
						'text' : 'SAM Information',
						'handler' : me.__oprShowSAMInformation,
						'properties' : {
							id : 'SAMInformation',
							tooltip : 'Click to show SAM information.'
						}
					}, {
						'text' : 'Host Information',
						'handler' : me.__oprShowHostInformation,
						'properties' : {
							id : 'HostInformation',
							tooltip : 'Click to show Host Job Information.'
						}
					}, {
						'text' : '-'
					}, {
						'text' : 'Send Test',
						'handler' : function() {
						},
						'properties' : {
							id : 'SendTest',
							tooltip : 'Click to send test for site.'
						}
					}]
		};

		me.contextGridMenu = Ext.create(
				'Ext.dirac.utils.DiracApplicationContextMenu', {
					menu : menuitems,
					scope : me
				});

		me.grid = Ext.create('Ext.dirac.utils.DiracGridPanel', {
					columnLines : true,
					oColumns : oColumns,
					pagingToolbar : pagingToolbar,
					contextMenu : me.contextGridMenu,
					store : me.datastore,
					scope : me
				});

		me.leftPanel.setGrid(me.grid);
		me.add([me.leftPanel, me.grid]);
	},

	__oprShowSAMInformation : function() {
		var me = this;

		oSite = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, 'Site');
		Ext.Ajax.request({
					url : GLOBAL.BASE_URL + me.applicationName + '/getSAMData',
					params : {
						site : '["' + oSite + '"]'
					},
					method : 'POST',
					scope : me,
					success : function(response) {
						var jsonData = Ext.JSON.decode(response.responseText);

						if (jsonData.success == 'false') {
							GLOBAL.APP.CF.msg("error", jsonData.error);
						} else {
							var oWindow = me.getContainer().createChildWindow(
									'SAM Information - ' + oSite, false, 500,
									300);

							var oFields = ['ElementName', 'ElementType',
									'WMS-Test', 'CVMFS-Test', 'BOSS-Test', 'SE-Test'];
							var oData = jsonData.result;
							var oStore = new Ext.data.Store({
										fields : oFields,
										data : oData
									});

							var oColumns = [{
										text : 'ElementName',
										flex : 1.2,
										dataIndex : 'ElementName'
									}, {
										text : 'ElementType',
										flex : 1,
										dataIndex : 'ElementType'
									}, {
										text : 'WMS',
										flex : 1,
										dataIndex : 'WMS-Test',
										renderer : me.__renderSAMStatus
									}, {
										text : 'CVMFS',
										flex : 1,
										dataIndex : 'CVMFS-Test',
										renderer : me.__renderSAMStatus
									}, {
										text : 'BOSS',
										flex : 1,
										dataIndex : 'BOSS-Test',
										renderer : me.__renderSAMStatus
									}, {
										text : 'SE',
										flex : 1,
										dataIndex : 'SE-Test',
										renderer : me.__renderSAMStatus
									}];

							var oGrid = Ext.create('Ext.grid.Panel', {
										store : oStore,
										region : 'center',
										columns : oColumns,
										viewConfig : {
											stripeRows : true,
											enableTextSelection : true
										}
									});
							oGrid.on('cellclick', me.__showSAMDetail, me);

							oWindow.add(oGrid);
							oWindow.show();
						}
					},
					failure : function(response) {
						GLOBAL.APP.CF.showAjaxErrorMessage(response);
					}
				});
	},

	__showSAMDetail : function(view, td, cellIndex, record, tr, rowIndex, e) {
		var me = this;

		if (cellIndex >= 2) {
			var oGrid = view.up('gridpanel');
			var oTestType = oGrid.columns[cellIndex].dataIndex;
			var oElementName = record.get(oGrid.columns[0].dataIndex);

			if (record.get(oTestType)) {
				Ext.Ajax.request({
					url : GLOBAL.BASE_URL + me.applicationName + '/getSAMDetail',
					params : {
						elementName : oElementName,
						testType : oTestType
					},
					method : 'POST',
					scope : me,
					success : function(response) {
						var jsonData = Ext.JSON.decode(response.responseText);

						if (jsonData.success == 'false') {
							GLOBAL.APP.CF.msg("error", jsonData.error);
						} else {
							var oData = jsonData.result;
							var oWindow = me.getContainer().createChildWindow(
									oElementName + ' - ' + oTestType, false,
									500, 300);
							var oForm = Ext.create('Ext.form.Panel', {
										title : 'SAM Detail Information',
										defaults : {
											labelAlign : 'right',
											margin : '5 10 5 10',
											anchor : '100%',
											readOnly : true
										},
										items : [{
													xtype : 'textfield',
													name : 'testType',
													fieldLabel : 'TestType',
													value : oData.TestType
												}, {
													xtype : 'textfield',
													name : 'elementName',
													fieldLabel : 'ElementName',
													value : oData.ElementName
												}, {
													xtype : 'textfield',
													name : 'elementType',
													fieldLabel : 'ElementType',
													value : oData.ElementType
												}, {
													xtype : 'textfield',
													name : 'submissionTime',
													fieldLabel : 'SubmissionTime',
													value : oData.SubmissionTime
												}, {
													xtype : 'textfield',
													name : 'completionTime',
													fieldLabel : 'CompletionTime',
													value : oData.CompletionTime
												}, {
													xtype : 'textfield',
													name : 'applicationTime',
													fieldLabel : 'ApplicationTime',
													value : oData.ApplicationTime
												}, {
													xtype : 'textfield',
													name : 'status',
													fieldLabel : 'Status',
													value : oData.Status
												}, {
													xtype : 'textareafield',
													name : 'log',
													fieldLabel : 'Log',
													anchor : '100% -190',
													value : oData.Log
												}]
									});

							oWindow.add(oForm);
							oWindow.show();
						}
					},
					failure : function(response) {
						GLOBAL.APP.CF.showAjaxErrorMessage(response);
					}
				});
			}
		}
	},

	__oprShowHostInformation : function(grid, record) {
		var me = this;

		oSite = GLOBAL.APP.CF.getFieldValueFromSelectedRow(me.grid, 'Site');
		Ext.Ajax.request({
					url : GLOBAL.BASE_URL + me.applicationName + '/getHostData',
					params : {
						site : '["' + oSite + '"]'
					},
					method : 'POST',
					scope : me,
					success : function(response) {
						var jsonData = Ext.JSON.decode(response.responseText);

						if (jsonData.success == 'false') {
							GLOBAL.APP.CF.msg("error", jsonData.error);
						} else {
							var oWindow = me.getContainer().createChildWindow(
									'Host Information - ' + oSite, false, 500,
									300);

							var oFields = ['Host', 'Running', 'Done', 'Failed',
									'Efficiency'];
							var oData = jsonData.result;
							var oStore = new Ext.data.Store({
										fields : oFields,
										data : oData
									});

							var oColumns = [{
										text : 'Host',
										flex : 1.2,
										dataIndex : 'Host'
									}, {
										text : 'Running',
										flex : 1,
										dataIndex : 'Running'
									}, {
										text : 'Done',
										flex : 1,
										dataIndex : 'Done'
									}, {
										text : 'Failed',
										flex : 1,
										dataIndex : 'Failed'
									}, {
										text : 'Efficiency',
										flex : 1,
										dataIndex : 'Efficiency'
									}];

							var oGrid = Ext.create('Ext.grid.Panel', {
										store : oStore,
										region : 'center',
										columns : oColumns,
										viewConfig : {
											stripeRows : true,
											enableTextSelection : true
										}
									});

							oWindow.add(oGrid);
							oWindow.show();
						}
					},
					failure : function(response) {
						GLOBAL.APP.CF.showAjaxErrorMessage(response);
					}
				})
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
