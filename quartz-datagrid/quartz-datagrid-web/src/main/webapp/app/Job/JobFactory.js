'use strict';

angular.module('jobModule').factory('jobFactory', ['$http','INFINISPAN_API_ENDPOINT',function($http, INFINISPAN_API_ENDPOINT) { 
	return {
		get : function() {
			var headers={ 'Content-Type' : 'application/json' };
			return $http.get(INFINISPAN_API_ENDPOINT + "/job" , headers);
		},
		create : function(message) {
			var headers={ 'Content-Type' : 'application/json' };
			return $http.put(INFINISPAN_API_ENDPOINT + "/job" , message , headers);
		}
	};
}]);
