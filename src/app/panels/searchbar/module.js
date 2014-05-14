define([
    'angular',
    'app',
    'lodash',
    'kbn',
    'moment'
  ],
  function(angular, app, _, kbn, moment) {
    'use strict';

    var module = angular.module('kibana.panels.searchbar', []);
    app.useModule(module);

    module.controller('searchbar', function($rootScope, $scope, $modal, $q, $compile, fields, querySrv, dashboard, filterSrv, $timeout) {

      $scope.panelMeta = {};

      $scope.init = function() {
        $scope.filterSrv = filterSrv;
        $scope.$on('refresh', function() {
          $scope.get_data();
        });
        $scope.get_data();
      };

      $scope.get_data = function() {
        $scope.searchText = "";
        _.each(_.filter(filterSrv.list(), function(filter) {
          return (filter.owner === undefined || filter.owner === "searchbar") && _.contains(['field', 'querystring'], filter.type);
        }), function(filter) {
          if (filter.type === "field" && filter.mandate === 'must') {
            if ($scope.searchText.trim().length > 0) {
              $scope.searchText += " AND "
            }
            $scope.searchText += filter["field"] + "=" + filter["query"];
          } else if (filter.type === "querystring") {
            if ($scope.searchText.trim().length > 0) {
              $scope.searchText += " AND "
            }
            $scope.searchText += filter["query"];
          }
        });
      };

      $scope.search = function() {
        var i = filterSrv.ids().length;
        while (i--) {
          if (!_.contains(['time', 'templatestring'], filterSrv.list()[i].type)) {
            if (filterSrv.list()[i].mandate === 'must') {
              filterSrv.remove(i,true);
            }
          }
        }
        if ($scope.searchText.trim().length > 0) {
          filterSrv.set({
            type: 'querystring',
            query: $scope.searchText.trim(),
            mandate: 'must',
            owner: "searchbar"
          },undefined,true);
        }
        
        $timeout(function(){
          dashboard.refresh();
        },0);          
      }
    });
  });