package com.bluesky.etl.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.bluesky.etl.util.common.JSONArray;
import com.bluesky.etl.util.common.JSONObject;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.gui.JobTracker;
import org.pentaho.di.core.logging.KettleLogLayout;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.logging.SimpleLoggingObject;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.*;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.ui.spoon.job.JobEntryCopyResult;
import org.pentaho.di.www.SlaveServerJobStatus;

public class JobExecutor implements Callable<String> {
	private String executionId;
	private JobExecutionConfiguration executionConfiguration;
	private JobMeta jobMeta = null;
	private Job job = null;
	private static final Class PKG = JobEntryCopyResult.class;
	private boolean finished = false;
	private long errCount = 0L;

	private JobExecutor(JobExecutionConfiguration executionConfiguration, JobMeta jobMeta) {
		this.executionId = UUID.randomUUID().toString().replaceAll("-", "");
		this.executionConfiguration = executionConfiguration;
		this.jobMeta = jobMeta;
	}

	private static Hashtable<String, JobExecutor> executors = new Hashtable();

	public static synchronized JobExecutor initExecutor(JobExecutionConfiguration executionConfiguration, JobMeta jobMeta) {
		JobExecutor jobExecutor = new JobExecutor(executionConfiguration, jobMeta);
		executors.put(jobExecutor.getExecutionId(), jobExecutor);
		return jobExecutor;
	}

	public String getExecutionId() {
		return this.executionId;
	}

	public String call() throws Exception {
		try {
			for (String varName : this.executionConfiguration.getVariables().keySet()) {
				String varValue = (String) this.executionConfiguration.getVariables().get(varName);
				this.jobMeta.setVariable(varName, varValue);
			}
			for (String paramName : this.executionConfiguration.getParams().keySet()) {
				String paramValue = (String) this.executionConfiguration.getParams().get(paramName);
				this.jobMeta.setParameterValue(paramName, paramValue);
			}
			//增加job监听方法
			/**
			this.job.addJobListener(new JobListener() {
				@Override
				public void jobFinished(Job job) throws KettleException {

				}

				@Override
				public void jobStarted(Job job) throws KettleException {

				}
			});
			*/
			if (this.executionConfiguration.isExecutingLocally()) {
				SimpleLoggingObject spoonLoggingObject = new SimpleLoggingObject("SPOON", LoggingObjectType.SPOON, null);
				spoonLoggingObject.setContainerObjectId(this.executionId);
				spoonLoggingObject.setLogLevel(this.executionConfiguration.getLogLevel());
				//this.job = new Job(EtlApp.getInstance().getRepository(), this.jobMeta, spoonLoggingObject);
				
				this.job = new Job(this.jobMeta.getRepository(), this.jobMeta, spoonLoggingObject);

				this.job.setLogLevel(this.executionConfiguration.getLogLevel());
				this.job.shareVariablesWith(this.jobMeta);
				this.job.setInteractive(true);
				this.job.setGatheringMetrics(this.executionConfiguration.isGatheringMetrics());
				this.job.setArguments(this.executionConfiguration.getArgumentStrings());

				this.job.getExtensionDataMap().putAll(this.executionConfiguration.getExtensionOptions());
				if (!Const.isEmpty(this.executionConfiguration.getStartCopyName())) {
					JobEntryCopy startJobEntryCopy = this.jobMeta.findJobEntry(this.executionConfiguration.getStartCopyName(), this.executionConfiguration.getStartCopyNr(), false);
					this.job.setStartJobEntryCopy(startJobEntryCopy);
				}
				Map<String, String> paramMap = this.executionConfiguration.getParams();
				Set<String> keys = paramMap.keySet();
				for (String key : keys) {
					this.job.getJobMeta().setParameterValue(key, Const.NVL((String) paramMap.get(key), ""));
				}
				this.job.getJobMeta().activateParameters();

				this.job.start();
				while (!this.job.isFinished()) {
					Thread.sleep(500L);
				}
				this.errCount = this.job.getErrors();
			} else if (this.executionConfiguration.isExecutingRemotely()) {
			//	this.carteObjectId = Job.sendToSlaveServer(this.jobMeta, this.executionConfiguration, EtlApp.getInstance().getRepository(), EtlApp.getInstance().getMetaStore());
				this.carteObjectId = Job.sendToSlaveServer(this.jobMeta, this.executionConfiguration, this.jobMeta.getRepository(), this.jobMeta.getRepository().getMetaStore());
				SlaveServer remoteSlaveServer = this.executionConfiguration.getRemoteServer();

				boolean running = true;
				while (running) {
					SlaveServerJobStatus jobStatus = remoteSlaveServer.getJobStatus(this.jobMeta.getName(), this.carteObjectId, 0);
					running = jobStatus.isRunning();
					if (!running) {
						this.errCount = jobStatus.getResult().getNrErrors();
					}
					Thread.sleep(500L);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			EtlApp.getInstance().getLog().logError("执行失败！", e);
		} finally {
			this.finished = true;
		}
		return "";
	}

	public boolean isFinished() {
		return this.finished;
	}

	public long getErrCount() {
		return this.errCount;
	}

	private String carteObjectId = null;
	public int previousNrItems;

	public JSONArray getJobMeasure() throws Exception {
		JSONArray jsonArray = new JSONArray();
		if (this.executionConfiguration.isExecutingLocally()) {
			JobTracker jobTracker = this.job.getJobTracker();
			int nrItems = jobTracker.getTotalNumberOfItems();
			if (nrItems != this.previousNrItems) {
				String jobName = jobTracker.getJobName();
				if (Const.isEmpty(jobName)) {
					if (!Const.isEmpty(jobTracker.getJobFilename())) {
						jobName = jobTracker.getJobFilename();
					} else {
						jobName = BaseMessages.getString(PKG, "JobLog.Tree.StringToDisplayWhenJobHasNoName", new String[0]);
					}
				}
				JSONObject jsonObject = new JSONObject();
				jsonObject.put("name", jobName);
				jsonObject.put("expanded", Boolean.valueOf(true));

				JSONArray children = new JSONArray();
				for (int i = 0; i < jobTracker.nrJobTrackers(); i++) {
					JSONObject jsonObject2 = addTrackerToTree(jobTracker.getJobTracker(i));
					if (jsonObject2 != null) {
						children.add(jsonObject2);
					}
				}
				jsonObject.put("children", children);
				jsonArray.add(jsonObject);

				this.previousNrItems = nrItems;
			}
		}
		return jsonArray;
	}

	private JSONObject addTrackerToTree(JobTracker jobTracker) {
		JSONObject jsonObject = new JSONObject();
		if (jobTracker != null) {
			if (jobTracker.nrJobTrackers() > 0) {
				jsonObject.put("name", BaseMessages.getString(PKG, "JobLog.Tree.JobPrefix", new String[0]) + jobTracker.getJobName());
				jsonObject.put("expanded", Boolean.valueOf(true));
				JSONArray children = new JSONArray();
				for (int i = 0; i < jobTracker.nrJobTrackers(); i++) {
					JSONObject jsonObject2 = addTrackerToTree(jobTracker.getJobTracker(i));
					if (jsonObject2 != null) {
						children.add(jsonObject2);
					}
				}
				jsonObject.put("children", children);
			} else {
				JobEntryResult result = jobTracker.getJobEntryResult();
				if (result != null) {
					String jobEntryName = result.getJobEntryName();
					if (!Const.isEmpty(jobEntryName)) {
						jsonObject.put("name", jobEntryName);
						jsonObject.put("fileName", Const.NVL(result.getJobEntryFilename(), ""));
					} else {
						jsonObject.put("name", BaseMessages.getString(PKG, "JobLog.Tree.JobPrefix2", new String[0]) + jobTracker.getJobName());
					}
					String comment = result.getComment();
					if (comment != null) {
						jsonObject.put("comment", comment);
					}
					Result res = result.getResult();
					if (res != null) {
						jsonObject.put("result", res.getResult() ? BaseMessages.getString(PKG, "JobLog.Tree.Success", new String[0]) : BaseMessages.getString(PKG, "JobLog.Tree.Failure", new String[0]));
						jsonObject.put("number", Long.toString(res.getEntryNr()));
					}
					String reason = result.getReason();
					if (reason != null) {
						jsonObject.put("reason", reason);
					}
					Date logDate = result.getLogDate();
					if (logDate != null) {
						jsonObject.put("logDate", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(logDate));
					}
					jsonObject.put("leaf", Boolean.valueOf(true));
				} else {
					return null;
				}
			}
		} else {
			return null;
		}
		return jsonObject;
	}

	public String getExecutionLog() throws Exception {
		if (this.executionConfiguration.isExecutingLocally()) {
			StringBuffer sb = new StringBuffer();
			KettleLogLayout logLayout = new KettleLogLayout(true);
			List<String> childIds = LoggingRegistry.getInstance().getLogChannelChildren(this.job.getLogChannelId());
			List<KettleLoggingEvent> logLines = KettleLogStore.getLogBufferFromTo(childIds, true, -1, KettleLogStore.getLastBufferLineNr());
			for (int i = 0; i < logLines.size(); i++) {
				KettleLoggingEvent event = (KettleLoggingEvent) logLines.get(i);
				String line = logLayout.format(event).trim();
				sb.append(line).append("\n");
			}
			return sb.toString();
		}
		SlaveServer remoteSlaveServer = this.executionConfiguration.getRemoteServer();
		SlaveServerJobStatus jobStatus = remoteSlaveServer.getJobStatus(this.jobMeta.getName(), this.carteObjectId, 0);
		return jobStatus.getLoggingString();
	}
}
