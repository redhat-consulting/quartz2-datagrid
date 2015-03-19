'use strict';

angular.module('jobModule').controller('jobController', ['jobFactory', '$log',function(jobFactory, $log) { 
	var vm = this;
	vm.message = "";
	vm.jobRecords = {};

	vm.create = function() {
		jobFactory.create(vm.message).then( function(response) {
			$log.info("response from create");
			$log.info(response);
			vm.init();
		});
	}
	
	vm.refresh = function () {
		jobFactory.get().then(function (response) {
			$log.info("reponse from job factory");
			$log.info(response.data);
			vm.jobRecords = response.data;
		});
	}
	
	vm.init = function() {
		vm.refresh();
	};
	
	vm.init();
}]);
