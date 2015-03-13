package com.rhc.quartz.rest.endpoint;

import java.util.List;

import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.rhc.quartz.entity.JobRecord;
import com.rhc.quartz.service.JobService;

@Path("/job")
@Produces({MediaType.APPLICATION_JSON})
@Stateless
public class JobEndpoint {
	
	@Inject
	private JobService jobService;

	@PUT
	public void createJob(String message) {
		jobService.createJob(message);
	}
	
	@GET
	public List<JobRecord> getAll() {
		return jobService.getJobRecords();
	}
}
