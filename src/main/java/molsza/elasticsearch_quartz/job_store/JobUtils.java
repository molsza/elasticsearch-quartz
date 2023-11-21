package molsza.elasticsearch_quartz.job_store;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.impl.JobDetailImpl;

/**
 * Provides utilities for managing jobs.
 *
 * @author Anton Johansson
 */
public final class JobUtils
{
	private JobUtils()
	{
	}
	
	/**
	 * Creates a {@link JobDetail} from a stored {@link JobWrapper}.
	 * 
	 * @param jobWrapper The wrapper to create job from.
	 * @return Returns the created {@link JobDetail}.
	 * 
	 * @throws JobPersistenceException Thrown if job class could not be found.
	 */
	public static JobDetail fromWrapper(JobWrapper jobWrapper) throws JobPersistenceException
	{
		String name = jobWrapper.getName();
		String group = jobWrapper.getGroup();
		
		JobDataMap jobDataMap = new JobDataMap(jobWrapper.getDataMap());
		
		JobDetailImpl job = new JobDetailImpl();
		job.setKey(new JobKey(name, group));
		job.setName(name);
    job.setDescription(jobWrapper.getDescription());
		job.setGroup(group);
		job.setJobClass(getJobClass(jobWrapper));
		job.setJobDataMap(jobDataMap);
		
		return job;
	}

	private static Class<? extends Job> getJobClass(JobWrapper jobWrapper) throws JobPersistenceException
	{
		String jobClass = jobWrapper.getJobClass();
		try
		{
			return Class.forName(jobClass).asSubclass(Job.class);
		}
		catch (ClassNotFoundException e)
		{
			throw new JobPersistenceException("Could not load job class '" + jobClass + "'");
		}
	}
}
