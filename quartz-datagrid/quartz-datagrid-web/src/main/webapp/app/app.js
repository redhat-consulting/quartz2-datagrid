'use strict';

angular.module('exampleApp', [ 'ngRoute', 'jobModule']).
  config(['$routeProvider', '$httpProvider',function($routeProvider, $httpProvider) {
    $routeProvider.when('/welcome', {templateUrl: 'app/Job/_job.html'});
    $routeProvider.otherwise({redirectTo: '/welcome'});
}]).constant('INFINISPAN_API_ENDPOINT','/quartz-datagrid-web/rest');
