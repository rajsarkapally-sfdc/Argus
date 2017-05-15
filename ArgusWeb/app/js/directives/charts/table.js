'use strict';
/*global angular:false */

angular.module('argus.directives.charts.table', [])
.directive('agTable', ['JsonFlattenService','$routeParams', 'DashboardService', 'DateHandlerService', 'AgTableService', 'growl', 'VIEWELEMENT', 'InputTracker', '$http', 'CONFIG', function( JsonFlattenService, $routeParams, DashboardService, DateHandlerService, AgTableService, growl, VIEWELEMENT, InputTracker, $http, CONFIG) {
	var tableNameIndex = 1;

	function setupTable(scope, element, controls){
		// remove/clear any previous chart rendering from DOM
		var lastEl = element.context.querySelector('[id^=element_table]');
		var lastId = lastEl? lastEl.id: null;
		// generate a new chart ID, set css options for main chart container
		//if the element has content previously, leave the id unchanged
		var newTableId = lastId || 'element_table' + tableNameIndex++;

		scope.tableId = newTableId;
		scope.tData = [];

		var GMTon = false;
		for (var i = 0; i < controls.length; i++) {
			if (controls[i].type === 'agDate') {
				var timeValue = controls[i].value;
				if (controls[i].name === 'start') {
					GMTon = GMTon || DateHandlerService.GMTVerifier(timeValue);
				} else if (controls[i].name === 'end'){
					GMTon = GMTon || DateHandlerService.GMTVerifier(timeValue);
				}
			}
		}
		scope.GMTOn = GMTon;

		// itemsPerPage setting
		var storageId = scope.dashboardId + '_' + newTableId;
		scope.itemsPerPageOptions = [5, 10, 15, 25, 50, 100, 200];
		var itemsPerPageFromStorage = storageId + '-itemsPerPage';
		scope.itemsPerPage = InputTracker.getDefaultValue(itemsPerPageFromStorage, scope.itemsPerPageOptions[1]);
		scope.$watch('itemsPerPage', function(newValue) {
			InputTracker.updateDefaultValue(itemsPerPageFromStorage, scope.itemsPerPageOptions[1], newValue);
			update();
		});

		// searchText setting
		var searchTextFromStorage = storageId + '-searchText';
		scope.searchText = InputTracker.getDefaultValue(searchTextFromStorage, '');
		scope.$watch('searchText', function(newValue) {
			InputTracker.updateDefaultValue(searchTextFromStorage, '', newValue);
		});

		// pagination page setting
		var currentPageFromStorage = storageId + '-currentPage';
		scope.currentPage = InputTracker.getDefaultValue(currentPageFromStorage, 1);
		scope.$watch('currentPage', function (newValue) {
			InputTracker.updateDefaultValue(currentPageFromStorage, 1, newValue);
			update();
		});

		// sort setting
		var sortKeyFromStorage = storageId + '-sortKey';
		scope.sortKey = InputTracker.getDefaultValue(sortKeyFromStorage, 'timestamp');
		var sortReverseFromStorage = storageId + '-sortReverse';
		scope.reverse = InputTracker.getDefaultValue(sortReverseFromStorage, 1);

		if(Math.abs(scope.reverse)!==1) scope.reverse = 1;

		scope.sort = function (item) {
			var key = item.timestamp;
			if (scope.sortKey === key) {
				scope.reverse = scope.reverse * -1;
				InputTracker.updateDefaultValue(sortReverseFromStorage, -1, scope.reverse);
			} else {
				scope.sortKey = key;
				InputTracker.updateDefaultValue(sortKeyFromStorage, 'timestamp', scope.sortKey);
			}
			scope.sortSourceIndices(item);
		};

		scope.sortSourceIndices = function(item){
			var sortedArray =[];
			for(var key in item)
			{
				if(key !== 'timestamp' && key !== 'datetime') sortedArray.push([key, item[key]]);
			}
			if(item['datetime'] === scope.colNames.datetime){
				sortedArray.sort(function(a, b){
					return (a[1].localeCompare(b[1])) * scope.reverse;
				});
			}else{
				sortedArray.sort(function(a, b){
					return (a[1] - b[1]) * scope.reverse;
				});
			}
			scope.sortedSourceIndices = sortedArray.map(function(d){ return d[0]; });
		};

		function update(){
			scope.start = (scope.currentPage - 1)* scope.itemsPerPage + 1;
			var end = scope.start + scope.itemsPerPage - 1;
			if (scope.series) {
				scope.end = end < scope.dataSet.length ? end : scope.dataSet.length;
			}
		}
	}

	function queryMetricData(scope, controls){
		scope.tableLoaded = false;
		var metricExpressionList = [];
		var key;
		for (key in scope.metrics) {
			if (scope.metrics.hasOwnProperty(key)) {
				// get metricExpression
				var metric = scope.metrics[key];
				var processedExpression = DashboardService.augmentExpressionWithControlsData(event, metric.expression, controls);
				metricExpressionList.push(processedExpression);
			}
		}

		scope.processedOptions = JsonFlattenService.unflatten(scope.options);

		$http({
			method: 'GET',
			url: CONFIG.wsUrl + 'metrics',
			params: {'expression': metricExpressionList}
		}).success(function(data) {
			if ( data && data.length > 0) {
				//set scope.tData and scope.colNames
				AgTableService.setTData(data, scope, scope.GMTOn);
				scope.tableLoaded = true;

			}else{
				console.log('No data found for the metric expressions: ' + JSON.stringify(metricExpressionList));
			}
		}).error(function(data) {
			growl.error(data.message);
		});
	}

	return {
		restrict: 'E',
		transclude: true,
		scope: {},
		require: '^agDashboard',
		controller: 'ViewElements',
		templateUrl: 'js/templates/ag-table.html',
		//template: '<div ng-transclude=""></div>',

		link: function(scope, element, attributes, dashboardCtrl) {
			//DashboardService.buildViewElement(scope, element, attributes, dashboardCtrl, VIEWELEMENT.table, tableNameIndex++, DashboardService, growl);

			scope.dashboardId = $routeParams.dashboardId;
			setupTable(scope, element, dashboardCtrl.getAllControls());
			queryMetricData(scope, dashboardCtrl.getAllControls());

			scope.$watch('tableLoaded', function(val){
				//do not use ng-show cause it does not update dom size in this digest
				if(val){
					angular.element(element.context.querySelector('.agTableDiv')).show();
				}else{
					angular.element(element.context.querySelector('.agTableDiv')).hide();
				}
			});

			scope.$watch(function(){
				return angular.element(element.context.querySelector('.agTableHeadRow th:nth-child(2)')).css('height'); },
					function(val){
						scope.headerHeight = val;
					}
			);

			angular.element(element.context.querySelector('table')).on('scroll', function(){
				scope.headerTop = this.scrollTop;
				scope.headerLeft = this.scrollLeft;
				scope.$apply();
			});

			scope.$on(dashboardCtrl.getSubmitBtnEventName(), function(event, controls) {
				delete scope.tData;
				var headerHeight = angular.element(element.context.querySelector('.agTableHead th:first-child')).css('height');
				angular.element(element.context.querySelector('.firstEmptyRow ')).css('height', headerHeight);
				queryMetricData(scope,controls);
			});

			element.on('$destroy', function(){
				tableNameIndex = 1;
			});
		}
	};
}]);