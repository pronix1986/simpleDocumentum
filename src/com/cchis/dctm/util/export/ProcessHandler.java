package com.cchis.dctm.util.export;


import com.cchis.dctm.util.export.exception.ExportException;
import com.cchis.dctm.util.export.util.FileUtil;
import com.cchis.dctm.util.export.util.Util;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.cchis.dctm.util.export.util.ExportConstants.*;

public final class ProcessHandler {

    private enum ServiceType {
        JBOSS, TARGET
    }

    private static final Logger LOG = Logger.getLogger(ProcessHandler.class);

    private static int resetThreshold = IDS_RESTART_DEFAULT_THRESHOLD;

    private static final String JAVA_EXE = "java.exe";
    private static final String CMD_EXE = "cmd.exe";
    private static final String RUN_BAT = "run.bat";
    private static final String DCTMSERVICE_EXE = "dctmservice.exe";
    private static final String SERVICECONFIG_EXE = "ServiceConfig.exe";
    private static final String STATE = "STATE";

    private static final String MSG_INFO_SERVICE_RESTARTED = "Service %s is successfully restarted";
    private static final String MSG_INFO_SERVICES_RESTARTED = "Service(s) %s is successfully restarted";
    private static final String MSG_ERROR_FAIL_TO_START_SERVICE_IN_TIME = "Failed to start the service %s in %d seconds. Current state is %s";

    private static final ConcurrentHashMap<ServiceId, ProcessHandler> map = new ConcurrentHashMap<>();
    private static final ServiceId targetServiceId1;
    private static final ServiceId sourceServiceId1;
    private static final ServiceId jmsServiceId1;
    private static  ServiceId targetServiceId2;
    private static ServiceId sourceServiceId2;
    private static ServiceId jmsServiceId2;
    private static final List<ProcessHandler> targetProcessHandlers = new ArrayList<>();
    private static final List<ProcessHandler> jbossProcessHandlers =  new ArrayList<>();

    static {
        PropertiesHolder properties = PropertiesHolder.getInstance();
        String targetHost1 = properties.getProperty(PROP_SCS_TARGET_HOST_1);
        String sourceHost1 = properties.getProperty(PROP_SCS_SOURCE_HOST_1);
        String targetHost2 = properties.getProperty(PROP_SCS_TARGET_HOST_2);
        String sourceHost2 = properties.getProperty(PROP_SCS_SOURCE_HOST_2);
        targetServiceId1 = new ServiceId(targetHost1, IDS_TARGET_SERVICE);
        sourceServiceId1 = new ServiceId(sourceHost1, IDS_SOURCE_SERVICE);
        jmsServiceId1 = new ServiceId(sourceHost1, JMS_SERVICE);
        if (StringUtils.isNotEmpty(targetHost2)) {
            targetServiceId2 = new ServiceId(targetHost2, IDS_TARGET_SERVICE);
        }
        if (StringUtils.isNotEmpty(sourceHost2)) {
            sourceServiceId2 = new ServiceId(sourceHost2, IDS_SOURCE_SERVICE);
            jmsServiceId2 = new ServiceId(sourceHost2, JMS_SERVICE);
        }

        targetProcessHandlers.add(ProcessHandler.getInstance(targetServiceId1));
        if (targetServiceId2 != null) {
            targetProcessHandlers.add(ProcessHandler.getInstance(targetServiceId2));
        }
        jbossProcessHandlers.add(ProcessHandler.getInstance(jmsServiceId1));
        jbossProcessHandlers.add(ProcessHandler.getInstance(sourceServiceId1));
        if (jmsServiceId2 != null) {
            jbossProcessHandlers.add(ProcessHandler.getInstance(jmsServiceId2));
        }
        if (sourceServiceId2 != null) {
            jbossProcessHandlers.add(ProcessHandler.getInstance(sourceServiceId2));
        }
        LOG.trace("ProcessHandler map: " + map);
    }

    private static final int STATE_UNKNOWN       = -1;
    private static final int STATE_STOPPED       = 1;
    private static final int STATE_START_PENDING = 2;
    private static final int STATE_STOP_PENDING  = 3;
    private static final int STATE_RUNNING       = 4;

    private static final String COMMAND = "cmd.exe /C sc \\\\%s %s \"%s\"";
    // TODO: use wmic instead
    // wmic /NODE:'...' service where (name='...') call startservice
    // wmic /NODE:'...' service where (name='...') call stopservice
    // wmic /NODE:'...' service where (name='...') get state [/format:csv]

    //private final static String CMD_LIST = "tasklist /FI \"imagename eq cmd.exe\" /V /FO LIST";
    private static final String DELETE_STALE_PROCESS_CMD = "wmic /NODE:'%s' process where (name='%s' and CommandLine like '%%%s%%' and CommandLine not like '%%ExportUtil%%') %s";
    private static final String GET_PROCESS_INFO_CMD =  "wmic /NODE:'%s' process where (processId='%s') get %s";




    private String host;
    private String serviceName;
    private ProcessId[] associatedProcesses;

    private ProcessHandler(ServiceId serviceId) {
        this.host = serviceId.getHost();
        this.serviceName = serviceId.getServiceName();
        this.associatedProcesses = serviceId.getProcesses();
    }

    private static synchronized ProcessHandler getInstance(String host, String serviceName) {
        ServiceId sId = new ServiceId(host, serviceName);
        return map.computeIfAbsent(sId, ProcessHandler::new);
    }

    private String getHost() {
        return host;
    }

    private String getServiceName() {
        return serviceName;
    }

    public ProcessId[] getAssociatedProcesses() {
        return associatedProcesses;
    }

    private static synchronized ProcessHandler getInstance(ServiceId id) {
        return getInstance(id.getHost(), id.getServiceName());
    }

    private int execService(String cmd) throws IOException {
        Process proc = Runtime.getRuntime().exec(String.format(COMMAND, host, cmd, serviceName));

        String stateLine = FileUtil.findStringByKey(proc.getInputStream(), STATE);
        int state = STATE_UNKNOWN;
        String stateStr = EMPTY_STRING;

        if (StringUtils.isNotEmpty(stateLine)) {
            state = Integer.parseInt(stateLine.substring(0, 1));
            stateStr = stateLine.substring(1).trim();
        }
        LOG.trace(String.format("Service %s has state %s %s", serviceName, state, stateStr));
        return state;
    }

    private int startService() throws IOException {
        LOG.trace(String.format("Starting service %s", serviceName));
        int state = queryService();
        if (STATE_RUNNING == state) {
            LOG.info(String.format("Failed to start %s: already running", serviceName));
            return state;
        }
        return execService("start");
    }

    private int stopService() throws IOException {
        ExecutorsHandler.waitBooksForCompletion(true);
        LOG.trace(String.format("Stopping service %s", serviceName));
        int state = queryService();
        if (STATE_STOPPED == state) {
            LOG.info(String.format("Failed to stop %s: already stopped", serviceName));
            return state;
        }
        return execService("stop");
    }

    private int stopService(int stopTimeout) throws IOException {
        stopService();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + stopTimeout * 1000) {
            int state = queryService();
            if (STATE_STOPPED == state) {
                return state;
            }
            Util.sleepSilently(DEFAULT_INTERVAL);
        }
        return queryService();
    }

    private void stopServiceAndCheck(int stopTimeout) throws IOException {
        int stopServiceState = stopService(stopTimeout);
        if (STATE_STOPPED != stopServiceState) {
            throw new ExportException(String.format("Failed to stop the service %s in %d seconds. Current state is %s", serviceName, stopTimeout, stopServiceState));
        }
    }

    private int startService(int startTimeout) throws IOException {
        startService();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + startTimeout * 1000) {
            int state = queryService();
            if (STATE_RUNNING == state) {
                return state;
            }
            Util.sleepSilently(DEFAULT_INTERVAL);
        }
        return queryService();
    }

    private void startServiceAndCheck(int startTimeout) throws IOException {
        int startServiceState = startService(startTimeout);
        if (STATE_RUNNING != startServiceState) {
            throw new ExportException(String.format(MSG_ERROR_FAIL_TO_START_SERVICE_IN_TIME, serviceName, startTimeout, startServiceState));
        }
    }

    private int queryService() throws IOException {
        return execService("query");
    }

    private void restartService(int stopTimeout, int startTimeout, int delay) throws IOException {
        stopServiceAndCheck(stopTimeout);
        startServiceAndCheck(startTimeout);
        Util.sleepSilently(delay);
        LOG.info(String.format(MSG_INFO_SERVICE_RESTARTED, serviceName));
    }

    private static void restartServices(List<ProcessHandler> handlers, int stopTimeout, int startTimeout, int delay) throws IOException, InterruptedException {
        List<String> sNames = new ArrayList<>();
        for (ProcessHandler ph : handlers) {
            ph.stopService(stopTimeout);
        }
        deleteStaleProcesses(handlers);
        for (ProcessHandler ph : handlers) {
            ph.startService(startTimeout);
            sNames.add(ph.getServiceName());
        }
        Util.sleepSilently(delay);
        LOG.info(String.format(MSG_INFO_SERVICES_RESTARTED, sNames));
    }

    private static void restartJBossServices() throws IOException, InterruptedException {
        restartServices(jbossProcessHandlers, JBOSS_STOP_TIMEOUT, JBOSS_START_TIMEOUT, JBOSS_DELAY);
    }

    private static void restartTargetServices() throws IOException, InterruptedException {
        restartServices(targetProcessHandlers, TARGET_STOP_TIMEOUT, TARGET_START_TIMEOUT, TARGET_DELAY);
    }

    private static void deleteStaleProcesses(List<ProcessHandler> phs) throws IOException, InterruptedException {
        LOG.debug("Start deleting stale processes");
        for (ProcessHandler ph: phs) {
            String host = ph.getHost();
            String command = "call terminate";
            Set<String> staleProcessesIds = ph.getStaleProcessIds();
            LOG.debug("Process ids to remove: " + staleProcessesIds);
            for (String id: staleProcessesIds) {
                LOG.trace(id + " : " + getProcessAttribute(host, id, "name") + " : " + getProcessAttribute(host, id, "CommandLine"));
            }

            if (Util.isNotEmptyCollection(staleProcessesIds)) {
                for(ProcessId processId: ph.getAssociatedProcesses()) {
                    String line = String.format(DELETE_STALE_PROCESS_CMD, host, processId.getProcessName(), processId.getClDetails(), command);
                    LOG.trace(line);
                    Runtime.getRuntime().exec(line).waitFor();
                }
            }
        }
        LOG.debug("End deleting state processes");
    }

    private Set<String> getStaleProcessIds() throws IOException {
        Set<String> ids = new HashSet<>();
        String command = "get processId";
        for (ProcessId processId : getAssociatedProcesses()) {
            ids.addAll(execGetProcessId(String.format(DELETE_STALE_PROCESS_CMD, host, processId.getProcessName(), processId.getClDetails(), command)));
        }
        return ids; // used only for logging
    }

    private static String getProcessAttribute(String host, String procId, String attr) throws IOException {
        String commandLine = String.format(GET_PROCESS_INFO_CMD, host, procId, attr);
        Process proc = Runtime.getRuntime().exec(commandLine);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream(), UTF_8_CS))) {
            return br.lines().map(String::trim).skip(1).collect(Collectors.joining());
        }
    }

    public static Set<String> execGetProcessId(String commandLine) throws IOException {
        Process proc = Runtime.getRuntime().exec(commandLine);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream(), UTF_8_CS))) {
            return br.lines().map(String::trim).filter(line -> StringUtils.isNotEmpty(line) && NumberUtils.isDigits(line)).collect(Collectors.toSet());
        }
    }

    private boolean isRunning() throws IOException {
        return STATE_RUNNING == queryService();
    }




    private static void restartServicesIfNeeded(int count) throws IOException, InterruptedException {
        ServerErrorCounter errorCounter = ServerErrorCounter.getInstance();
        int errorCount = errorCounter.getErrorCount();
        LOG.trace(String.format("Current count: %d. Error count: %d", count, errorCount));
        boolean errorThresholdExceeded = errorCount >= IDS_RESTART_ERROR_THRESHOLD;
        if (errorThresholdExceeded) {
            LOG.info(String.format(MSG_ERROR_COUNT_THRESHOLD_EXCEEDED, errorCount, IDS_RESTART_ERROR_THRESHOLD));
            errorCounter.resetCounter();
        }
        if (errorThresholdExceeded || (count >= resetThreshold)) {
            LOG.trace("Restarting IDS Services. Error Count: " + errorCount
                    + ". ThresholdExceeded: " + BooleanUtils.toStringYesNo(errorThresholdExceeded)
                    + ". Count: " + count + ". ResetThreshold: " + resetThreshold);
            restartTargetServices();
            restartJBossServices();
            resetThreshold = (count > 0) ? count + IDS_RESTART_DEFAULT_THRESHOLD : resetThreshold;
        }
    }

    private static boolean servicesRunning(List<ProcessHandler> phs) throws IOException {
        for (ProcessHandler ph : phs) {
            if (!ph.isRunning()) return false;
        }
        return true;
    }

    private static boolean jbossServicesRunning() throws IOException {
        return servicesRunning(jbossProcessHandlers);
    }

    private static boolean targetServicesRunning() throws IOException {
        return servicesRunning(targetProcessHandlers);
    }

    public static void checkServices(int count) {
        try {
            if (!jbossServicesRunning()) {
                ProcessHandler.restartJBossServices();
            }
            if (!targetServicesRunning()) {
                ProcessHandler.restartTargetServices();
            }
            restartServicesIfNeeded(count);
        } catch (Exception e) {
            throw new ExportException("checkServices()", e);
        }
    }

    @Override
    public String toString() {
        return "ProcessHandler{" +
                "host='" + host + '\'' +
                ", serviceName='" + serviceName + '\'' +
                '}';
    }

    static class ServiceId {
        final String host;
        final String serviceName;
        final ProcessId[] associatedProcesses;

        public ServiceId(String host, String serviceName) {
            this.host = host;
            this.serviceName = serviceName;
            this.associatedProcesses = initAssociatedProcesses(serviceName);
        }

        String getHost() {
            return host;
        }

        String getServiceName() {
            return serviceName;
        }

        public ProcessId[] getProcesses() {
            return associatedProcesses;
        }

        ProcessId[] initAssociatedProcesses(String serviceName) {
            switch (getServiceType(serviceName)) {
                case JBOSS: return getJbossProcessIds(serviceName);
                case TARGET: return getTargetProcessIds(serviceName);
                default: throw new ExportException("Unknown serviceType");
            }
        }

        ServiceType getServiceType(String serviceName) {
            if(IDS_TARGET_SERVICE.equalsIgnoreCase(serviceName)) return ServiceType.TARGET;
            if(IDS_SOURCE_SERVICE.equalsIgnoreCase(serviceName) || JMS_SERVICE.equalsIgnoreCase(serviceName)) return ServiceType.JBOSS;
            throw new ExportException("Unknown serviceType");
        }

        ProcessId[] getTargetProcessIds(String serviceName) {
            return new ProcessId[] {new ProcessId(SERVICECONFIG_EXE, SERVICECONFIG_EXE)};
        }

        ProcessId[] getJbossProcessIds(String serviceName) {
            String clName = serviceName.substring(2, serviceName.length());
            return new ProcessId[]{
                    new ProcessId(DCTMSERVICE_EXE, clName),
                    new ProcessId(CMD_EXE, clName),
                    new ProcessId(JAVA_EXE, clName),
                    new ProcessId(JAVA_EXE, RUN_BAT)
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServiceId serviceId = (ServiceId) o;
            return Objects.equals(host, serviceId.host) &&
                    Objects.equals(serviceName, serviceId.serviceName) &&
                    Arrays.equals(associatedProcesses, serviceId.associatedProcesses);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(host, serviceName);
            result = 31 * result + Arrays.hashCode(associatedProcesses);
            return result;
        }
    }


    static class ProcessId { // Don't use pid
        final String processName;
        final String clDetails;

        ProcessId(String processName, String clDetails) {
            this.processName = processName;
            this.clDetails = clDetails;
        }

        public String getProcessName() {
            return processName;
        }

        public String getClDetails() {
            return clDetails;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessId processId = (ProcessId) o;
            return Objects.equals(processName, processId.processName) &&
                    Objects.equals(clDetails, processId.clDetails);
        }

        @Override
        public int hashCode() {
            return Objects.hash(processName, clDetails);
        }
    }
}
