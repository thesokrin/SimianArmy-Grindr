/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.simianarmy.basic.janitor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.simianarmy.*;
import com.netflix.simianarmy.MonkeyRecorder.Event;
import com.netflix.simianarmy.janitor.AbstractJanitor;
import com.netflix.simianarmy.janitor.JanitorEmailNotifier;
import com.netflix.simianarmy.janitor.JanitorMonkey;
import com.netflix.simianarmy.janitor.JanitorResourceTracker;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;

//S3 Bucket Upload
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.AmazonServiceException;

/** The basic implementation of Janitor Monkey. */
public class BasicJanitorMonkey extends JanitorMonkey {

	Date date = new Date();
	long timeStamp = date.getTime();
	String fileDir = "csv/"+timeStamp+"/";

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicJanitorMonkey.class);

    /** The Constant NS. */
    private static final String NS = "simianarmy.janitor.";

    /** 	The cfg. */
    private final MonkeyConfiguration cfg;

    private final List<AbstractJanitor> janitors;

    private final JanitorEmailNotifier emailNotifier;

    private final String region;

    private final String accountName;
	
    private final JanitorResourceTracker resourceTracker;

    private final MonkeyRecorder recorder;

    private final MonkeyCalendar calendar;
    
    /** Keep track of the number of monkey runs */
    protected final AtomicLong monkeyRuns = new AtomicLong(0);    

    /** Keep track of the number of monkey errors */
    protected final AtomicLong monkeyErrors = new AtomicLong(0);    
    
    /** Emit a servor signal to track the running monkey */
    protected final AtomicLong monkeyRunning = new AtomicLong(0);    
    
    /**
     * Instantiates a new basic janitor monkey.
     *
     * @param ctx
     *            the ctx
     */
    public BasicJanitorMonkey(Context ctx) {
        super(ctx);
        this.cfg = ctx.configuration();

        janitors = ctx.janitors();
        emailNotifier = ctx.emailNotifier();
        region = ctx.region();
        accountName = ctx.accountName();
        resourceTracker = ctx.resourceTracker();
        recorder = ctx.recorder();
        calendar = ctx.calendar();

        // register this janitor with servo
        Monitors.registerObject("simianarmy.janitor", this);                
    }

    /** {@inheritDoc} */
    @Override
    public void doMonkeyBusiness() {
        cfg.reload();
        context().resetEventReport();

        if (!isJanitorMonkeyEnabled()) {
            return;
        } else {
            LOGGER.info(String.format("Marking resources with %d janitors.", janitors.size()));
            monkeyRuns.incrementAndGet();
            monkeyRunning.set(1);

            // prepare to run, this just resets the counts so monitoring is sane
            for (AbstractJanitor janitor : janitors) {
            	janitor.prepareToRun();
            }

            for (AbstractJanitor janitor : janitors) {
                LOGGER.info(String.format("Running %s janitor for region %s", janitor.getResourceType(), janitor.getRegion()));
                try {
                	janitor.markResources();
                } catch (Exception e) {
                	monkeyErrors.incrementAndGet();
                	LOGGER.error(String.format("Got an exception while %s janitor was marking for region %s", janitor.getResourceType(), janitor.getRegion()), e);
                }
                LOGGER.info(String.format("Marked %d resources of type %s in the last run.",
                        janitor.getMarkedResources().size(), janitor.getResourceType().name()));
                LOGGER.info(String.format("Unmarked %d resources of type %s in the last run.",
                        janitor.getUnmarkedResources().size(), janitor.getResourceType()));
            }

            if (!cfg.getBoolOrElse("simianarmy.janitor.leashed", true)) {
                emailNotifier.sendNotifications();
            } else {
		LOGGER.info("Janitor Monkey is leashed, no notification is sent.");
            }

            LOGGER.info(String.format("Cleaning resources with %d janitors.", janitors.size()));
            for (AbstractJanitor janitor : janitors) {
            	try {
            		janitor.cleanupResources();
                } catch (Exception e) {
                	monkeyErrors.incrementAndGet();
                	LOGGER.error(String.format("Got an exception while %s janitor was cleaning for region %s", janitor.getResourceType(), janitor.getRegion()), e);
                }
                LOGGER.info(String.format("Cleaned %d resources of type %s in the last run.",
                        janitor.getCleanedResources().size(), janitor.getResourceType()));
                LOGGER.info(String.format("Failed to clean %d resources of type %s in the last run.",
                        janitor.getFailedToCleanResources().size(), janitor.getResourceType()));
            }
            if (cfg.getBoolOrElse(NS + "summaryEmail.enabled", true)) {
	    	LOGGER.info("Janitor Monkey is generating a summary email....");
                sendJanitorSummaryEmail();
            }
        	monkeyRunning.set(0);
        }
    }

    @Override
    public Event optInResource(String resourceId) {
        return optInOrOutResource(resourceId, true, region);
    }

    @Override
    public Event optOutResource(String resourceId) {
        return optInOrOutResource(resourceId, false, region);
    }

    @Override
    public Event optInResource(String resourceId, String resourceRegion) {
        return optInOrOutResource(resourceId, true, resourceRegion);
    }

    @Override
    public Event optOutResource(String resourceId, String resourceRegion) {
        return optInOrOutResource(resourceId, false, resourceRegion);
    }

    private Event optInOrOutResource(String resourceId, boolean optIn, String resourceRegion) {
        if (resourceRegion == null) {
            resourceRegion = region;
        }

        Resource resource = resourceTracker.getResource(resourceId, resourceRegion);
        if (resource == null) {
            return null;
        }

        EventTypes eventType = optIn ? EventTypes.OPT_IN_RESOURCE : EventTypes.OPT_OUT_RESOURCE;
        long timestamp = calendar.now().getTimeInMillis();
        // The same resource can have multiple events, so we add the timestamp to the id.
        Event evt = recorder.newEvent(Type.JANITOR, eventType, resource, resourceId + "@" + timestamp);
        recorder.recordEvent(evt);
        resource.setOptOutOfJanitor(!optIn);
        resourceTracker.addOrUpdate(resource);
        return evt;
    }

//pull these from the properties file eventually along with data storage/distribution options for csv files
String bucket_name = "";
String key_name = "";
String file_name = "";

    /**
     * Send a summary email with about the last run of the janitor monkey.
     */
    protected void sendJanitorSummaryEmail() {
        String summaryEmailTarget = cfg.getStr(NS + "summaryEmail.to");
        if (!StringUtils.isEmpty(summaryEmailTarget)) {
            if (!emailNotifier.isValidEmail(summaryEmailTarget)) {
                LOGGER.error(String.format("The email target address '%s' for Janitor summary email is invalid",
                        summaryEmailTarget));
                return;
            }
            StringBuilder message = new StringBuilder();
	    // Email Header
	    File dir = new File("csv/"+timeStamp+"/");
	    dir.mkdir();
            message.append("<center><img height='150' src='http://www.silverelitez.org/jm.jpg'><br>");
	    // Email Body
            for (AbstractJanitor janitor : janitors) {
                ResourceType resourceType = janitor.getResourceType();
                appendSummary(message, "markings", resourceType, janitor.getMarkedResources(), janitor.getRegion(), "blue");
                appendSummary(message, "unmarkings", resourceType, janitor.getUnmarkedResources(), janitor.getRegion(), "orange");
                appendSummary(message, "cleanups", resourceType, janitor.getCleanedResources(), janitor.getRegion(), "green");
                appendSummary(message, "failures", resourceType, janitor.getFailedToCleanResources(), janitor.getRegion(), "red");
            }
	    // Email Footer
	    message.append(String.format("<br>Timestamp:%s<br>Root URL:%s<br>https://github.com/Netflix/SimianArmy/wiki", timeStamp, fileDir));

            String subject = getSummaryEmailSubject();
            emailNotifier.sendEmail(summaryEmailTarget, subject, message.toString());
        }
    }

    private void appendSummary(StringBuilder message, String summaryName,
	        	ResourceType resourceType, Collection<Resource> resources, String janitorRegion, String color) {
        	message.append(String.format("<h3>Total <font color='%s'>%s</font> for %s = <b>%d</b> in region %s</h3>",
               	color, summaryName, resourceType.name(), resources.size(), janitorRegion));
		CSVWriter writer = null;
		// make directory
		File dir = new File("csv/"+timeStamp+"/"+janitorRegion+"/");
		dir.mkdir();
		String fileName = dir.toString() + "/" + summaryName + "-" + resourceType.name() + "-janitormonkey-grindr-preprod.csv";
		String csvUrl = "http://"+bucket_name+"/"+ fileName;
		// insert url to csv for following table
		message.append(String.format("<a href='%s'>CSV file for %s %s</a><br><br>", csvUrl, resourceType.name(), summaryName));
		// attempt to create the directory here
		// open csv file for writing
		try {
			writer = new CSVWriter(new FileWriter(dir.toString() + "/" + summaryName + "-" + resourceType.name() + "-janitormonkey-grindr-preprod.csv"), ',');
		} catch (IOException ioexception) { ioexception.printStackTrace(); System.exit(1); }
		// TABLE COLUMN NAMES
		String[] resourceDataHeader = {"Resource ID","Name","Project Owner","Atlas Owner",
						"Atlas Email","Atlas Environment",
		  				"Atlas Zone","Termination Reason",
						"Launch Time","Termination Time"};
		writer.writeNext(resourceDataHeader);
		message.append("<table border='2' cellpadding='4'><tr>");
	    	for (String resource : resourceDataHeader) {
			message.append(String.format("<td bgcolor='grey'>%s</td>", resource));
		}
		message.append(String.format("</tr>%s", printResources(resources, writer)));
    }

    private String printResources(Collection<Resource> resources, CSVWriter writer) {
        StringBuilder sb = new StringBuilder();
	if (resources != null && resources.size() != 0) {
	        for (Resource r : resources) {
		// add region to directory structure
		    // TABLE VALUES
	   	    String[] resourceData = {r.getId(),r.getTag("Name"),r.getTag("project_owner"),r.getTag("atlas_owner"),
						r.getTag("atlas_owner")+"@grindr.com",r.getTag("atlas_environment"),
						r.getTag("atlas_zone"),r.getTerminationReason(),
						r.getLaunchTime().toString(),r.getExpectedTerminationTime().toString()};
		    writer.writeNext(resourceData);
		    sb.append("<tr>");
		    String color = "black";
		    for (String resource : resourceData) {
			if ( resource != null ) { color = "black"; } else { color = "red"; }
			sb.append(String.format("<td><font color='%s'>%s</font></td>", color, resource));
		    }
		    sb.append("</tr>");
	        }
	} else {
		sb.append("-No resources to list-");
	}
	try { writer.close();} catch (IOException ioexception) {ioexception.printStackTrace(); System.exit(1);}

//	    final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
//	    try {
//		s3.putObject(bucket_name, key_name, file_path);
//	    } catch (AmazonServiceException e) {
//		System.err.println(e.getErrorMessage());
//  		System.exit(1);
//	    }

	sb.append("</table>");
        return sb.toString();
    }

    /**
     * Gets the summary email subject for the last run of janitor monkey.
     * @return the subject of the summary email
     */
    protected String getSummaryEmailSubject() {
        return String.format("Janitor monkey execution summary (%s, %s)", accountName, region);
    }

    /**
     * Handle cleanup error. This has been abstracted so subclasses can decide to continue causing chaos if desired.
     *
     * @param resource
     *            the instance
     * @param e
     *            the exception
     */
    protected void handleCleanupError(Resource resource, Throwable e) {
        String msg = String.format("Failed to clean up %s resource %s with error %s",
                resource.getResourceType(), resource.getId(), e.getMessage());
        LOGGER.error(msg);
        throw new RuntimeException(msg, e);
    }

    private boolean isJanitorMonkeyEnabled() {
        String prop = NS + "enabled";
        if (cfg.getBoolOrElse(prop, true)) {
            return true;
        }
        LOGGER.info("JanitorMonkey disabled, set {}=true", prop);
        return false;
    }    
    @Monitor(name="runs", type=DataSourceType.COUNTER)
    public long getMonkeyRuns() {
      return monkeyRuns.get();
    }

    @Monitor(name="errors", type=DataSourceType.GAUGE)
    public long getMonkeyErrors() {
      return monkeyErrors.get();
    }

    @Monitor(name="running", type=DataSourceType.GAUGE)
    public long getMonkeyRunning() {
      return monkeyRunning.get();
    }
}
