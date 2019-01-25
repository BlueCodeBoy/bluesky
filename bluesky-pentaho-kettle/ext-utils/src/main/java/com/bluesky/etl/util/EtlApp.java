package com.bluesky.etl.util;

import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.logging.*;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.job.JobExecutionConfiguration;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;

import java.util.ArrayList;

public class EtlApp {

	private static EtlApp app;
	private LogChannelInterface log;
	
	private TransExecutionConfiguration transExecutionConfiguration;
	private TransExecutionConfiguration transPreviewExecutionConfiguration;
	private TransExecutionConfiguration transDebugExecutionConfiguration;
	private JobExecutionConfiguration jobExecutionConfiguration;


	private boolean logined=false;
	
	public PropsWebUI props;

	private EtlApp() {
		props = PropsWebUI.getInstance();
		log = new LogChannel( PropsWebUI.getAppName());
		loadSettings();
		
		transExecutionConfiguration = new TransExecutionConfiguration();
	    transExecutionConfiguration.setGatheringMetrics( true );
	    transPreviewExecutionConfiguration = new TransExecutionConfiguration();
	    transPreviewExecutionConfiguration.setGatheringMetrics( true );
	    transDebugExecutionConfiguration = new TransExecutionConfiguration();
	    transDebugExecutionConfiguration.setGatheringMetrics( true );

	    jobExecutionConfiguration = new JobExecutionConfiguration();
	    
	    variables = new RowMetaAndData( new RowMeta() );
		logined=false;
	}
	
	public void loadSettings() {
		LogLevel logLevel = LogLevel.getLogLevelForCode(props.getLogLevel());
		DefaultLogLevel.setLogLevel(logLevel);
		log.setLogLevel(logLevel);
		KettleLogStore.getAppender().setMaxNrLines(props.getMaxNrLinesInLog());

		// transMeta.setMaxUndo(props.getMaxUndo());
		DBCache.getInstance().setActive(props.useDBCache());
	}

	public static EtlApp getInstance() {
		if (app == null) {
			app = new EtlApp();
		}
		return app;
	}

	private Repository repository;

	public Repository getRepository() {
		return repository;
	}

	private Repository defaultRepository;
	
	public void initDefault(Repository defaultRepo) {
		if(this.defaultRepository == null)
			this.defaultRepository = defaultRepo;
		this.repository = defaultRepo;
	}
	
	public Repository getDefaultRepository() {
		return this.defaultRepository;
	}
	
	public void selectRepository(Repository repo) {
		if(repository != null) {
			repository.disconnect();
		}
		repository = repo;
	}

	private DelegatingMetaStore metaStore;

	public DelegatingMetaStore getMetaStore() {
		return metaStore;
	}
	
	public LogChannelInterface getLog() {
		return log;
	}
	
	private RowMetaAndData variables = null;
	private ArrayList<String> arguments = new ArrayList<String>();
	
	public String[] getArguments() {
		return arguments.toArray(new String[arguments.size()]);
	}
	
	public JobExecutionConfiguration getJobExecutionConfiguration() {
		return jobExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransDebugExecutionConfiguration() {
		return transDebugExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransPreviewExecutionConfiguration() {
		return transPreviewExecutionConfiguration;
	}

	public TransExecutionConfiguration getTransExecutionConfiguration() {
		return transExecutionConfiguration;
	}
	
	public RowMetaAndData getVariables() {
		return variables;
	}

	public boolean isLogined() {
		return logined;
	}

	public void setLogined(boolean logined) {
		this.logined = logined;
	}


}
