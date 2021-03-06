/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.worker.http;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.BaseServerNode;
import com.metamx.druid.client.ClientConfig;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.client.MutableServerView;
import com.metamx.druid.client.OnlyNewSegmentWatcherServerView;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.StatusServlet;
import com.metamx.druid.initialization.CuratorConfig;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServerConfig;
import com.metamx.druid.initialization.ServerInit;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.S3DataSegmentKiller;
import com.metamx.druid.merger.common.RetryPolicyFactory;
import com.metamx.druid.merger.common.TaskToolboxFactory;
import com.metamx.druid.merger.common.actions.RemoteTaskActionClientFactory;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.config.RetryPolicyConfig;
import com.metamx.druid.merger.common.config.TaskConfig;
import com.metamx.druid.merger.common.index.StaticS3FirehoseFactory;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.druid.merger.worker.WorkerCuratorCoordinator;
import com.metamx.druid.merger.worker.WorkerTaskMonitor;
import com.metamx.druid.merger.worker.config.WorkerConfig;
import com.metamx.druid.realtime.SegmentAnnouncer;
import com.metamx.druid.realtime.ZkSegmentAnnouncer;
import com.metamx.druid.realtime.ZkSegmentAnnouncerConfig;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitters;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.MonitorScheduler;
import com.metamx.metrics.MonitorSchedulerConfig;
import com.metamx.metrics.SysMonitor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.Duration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.skife.config.ConfigurationObjectFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class WorkerNode extends BaseServerNode<WorkerNode>
{
  private static final EmittingLogger log = new EmittingLogger(WorkerNode.class);

  public static Builder builder()
  {
    return new Builder();
  }

  private final Lifecycle lifecycle;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private RestS3Service s3Service = null;
  private List<Monitor> monitors = null;
  private HttpClient httpClient = null;
  private ServiceEmitter emitter = null;
  private TaskConfig taskConfig = null;
  private WorkerConfig workerConfig = null;
  private DataSegmentPusher segmentPusher = null;
  private TaskToolboxFactory taskToolboxFactory = null;
  private CuratorFramework curatorFramework = null;
  private ServiceDiscovery serviceDiscovery = null;
  private ServiceProvider coordinatorServiceProvider = null;
  private WorkerCuratorCoordinator workerCuratorCoordinator = null;
  private WorkerTaskMonitor workerTaskMonitor = null;
  private MutableServerView newSegmentServerView = null;
  private Server server = null;

  private boolean initialized = false;

  public WorkerNode(
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);

    this.lifecycle = lifecycle;
    this.props = props;
    this.configFactory = configFactory;
  }

  public WorkerNode setHttpClient(HttpClient httpClient)
  {
    this.httpClient = httpClient;
    return this;
  }

  public WorkerNode setEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
    return this;
  }

  public WorkerNode setS3Service(RestS3Service s3Service)
  {
    this.s3Service = s3Service;
    return this;
  }

  public WorkerNode setSegmentPusher(DataSegmentPusher segmentPusher)
  {
    this.segmentPusher = segmentPusher;
    return this;
  }

  public WorkerNode setTaskToolboxFactory(TaskToolboxFactory taskToolboxFactory)
  {
    this.taskToolboxFactory = taskToolboxFactory;
    return this;
  }

  public WorkerNode setCuratorFramework(CuratorFramework curatorFramework)
  {
    this.curatorFramework = curatorFramework;
    return this;
  }

  public WorkerNode setCoordinatorServiceProvider(ServiceProvider coordinatorServiceProvider)
  {
    this.coordinatorServiceProvider = coordinatorServiceProvider;
    return this;
  }

  public WorkerNode setServiceDiscovery(ServiceDiscovery serviceDiscovery)
  {
    this.serviceDiscovery = serviceDiscovery;
    return this;
  }

  public WorkerNode setWorkerCuratorCoordinator(WorkerCuratorCoordinator workerCuratorCoordinator)
  {
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    return this;
  }

  public WorkerNode setNewSegmentServerView(MutableServerView newSegmentServerView)
  {
    this.newSegmentServerView = newSegmentServerView;
    return this;
  }

  public WorkerNode setWorkerTaskMonitor(WorkerTaskMonitor workerTaskMonitor)
  {
    this.workerTaskMonitor = workerTaskMonitor;
    return this;
  }

  @Override
  public void doInit() throws Exception
  {
    initializeHttpClient();
    initializeEmitter();
    initializeS3Service();
    initializeMonitors();
    initializeMergerConfig();
    initializeCuratorFramework();
    initializeServiceDiscovery();
    initializeCoordinatorServiceProvider();
    initializeNewSegmentServerView();
    initializeDataSegmentPusher();
    initializeTaskToolbox();
    initializeJacksonInjections();
    initializeJacksonSubtypes();
    initializeCuratorCoordinator();
    initializeWorkerTaskMonitor();
    initializeServer();

    final ScheduledExecutorFactory scheduledExecutorFactory = ScheduledExecutors.createFactory(lifecycle);
    final ScheduledExecutorService globalScheduledExec = scheduledExecutorFactory.create(1, "Global--%d");
    final MonitorScheduler monitorScheduler = new MonitorScheduler(
        configFactory.build(MonitorSchedulerConfig.class),
        globalScheduledExec,
        emitter,
        monitors
    );
    lifecycle.addManagedInstance(monitorScheduler);

    final Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new StatusServlet()), "/status");
    root.addServlet(new ServletHolder(new DefaultServlet()), "/mmx/*");
    root.addServlet(
        new ServletHolder(
            new QueryServlet(getJsonMapper(), getSmileMapper(), workerTaskMonitor, emitter, getRequestLogger())
        ),
        "/druid/v2/*"
    );
    root.addFilter(GuiceFilter.class, "/mmx/indexer/worker/v1/*", 0);
  }

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (!initialized) {
      init();
    }

    lifecycle.start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    lifecycle.stop();
  }

  private void initializeServer()
  {
    if (server == null) {
      server = Initialization.makeJettyServer(configFactory.build(ServerConfig.class));

      lifecycle.addHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              log.info("Starting Jetty");
              server.start();
            }

            @Override
            public void stop()
            {
              log.info("Stopping Jetty");
              try {
                server.stop();
              }
              catch (Exception e) {
                log.error(e, "Exception thrown while stopping Jetty");
              }
            }
          }
      );
    }
  }

  private void initializeJacksonInjections()
  {
    InjectableValues.Std injectables = new InjectableValues.Std();

    injectables.addValue("s3Client", s3Service)
               .addValue("segmentPusher", segmentPusher);

    getJsonMapper().setInjectableValues(injectables);
  }

  private void initializeJacksonSubtypes()
  {
    getJsonMapper().registerSubtypes(StaticS3FirehoseFactory.class);
  }

  private void initializeHttpClient()
  {
    if (httpClient == null) {
      httpClient = HttpClientInit.createClient(
          HttpClientConfig.builder().withNumConnections(1)
                          .withReadTimeout(new Duration(PropUtils.getProperty(props, "druid.emitter.timeOut")))
                          .build(), lifecycle
      );
    }
  }

  private void initializeEmitter()
  {
    if (emitter == null) {
      emitter = new ServiceEmitter(
          PropUtils.getProperty(props, "druid.service"),
          PropUtils.getProperty(props, "druid.host"),
          Emitters.create(props, httpClient, getJsonMapper(), lifecycle)
      );
    }
    EmittingLogger.registerEmitter(emitter);
  }

  private void initializeS3Service() throws S3ServiceException
  {
    if (s3Service == null) {
      s3Service = new RestS3Service(
          new AWSCredentials(
              PropUtils.getProperty(props, "com.metamx.aws.accessKey"),
              PropUtils.getProperty(props, "com.metamx.aws.secretKey")
          )
      );
    }
  }

  private void initializeMonitors()
  {
    if (monitors == null) {
      monitors = Lists.newArrayList();
      monitors.add(new JvmMonitor());
      monitors.add(new SysMonitor());
    }
  }

  private void initializeMergerConfig()
  {
    if (taskConfig == null) {
      taskConfig = configFactory.build(TaskConfig.class);
    }

    if (workerConfig == null) {
      workerConfig = configFactory.build(WorkerConfig.class);
    }
  }

  public void initializeDataSegmentPusher()
  {
    if (segmentPusher == null) {
      segmentPusher = ServerInit.getSegmentPusher(props, configFactory, getJsonMapper());
    }
  }

  public void initializeTaskToolbox() throws S3ServiceException
  {
    if (taskToolboxFactory == null) {
      final DataSegmentKiller dataSegmentKiller = new S3DataSegmentKiller(s3Service);
      final SegmentAnnouncer segmentAnnouncer = new ZkSegmentAnnouncer(
          configFactory.build(ZkSegmentAnnouncerConfig.class),
          getPhoneBook()
      );
      lifecycle.addManagedInstance(segmentAnnouncer);
      taskToolboxFactory = new TaskToolboxFactory(
          taskConfig,
          new RemoteTaskActionClientFactory(
              httpClient,
              coordinatorServiceProvider,
              new RetryPolicyFactory(
                  configFactory.buildWithReplacements(
                      RetryPolicyConfig.class,
                      ImmutableMap.of("base_path", "druid.worker.taskActionClient")
                  )
              ),
              getJsonMapper()
          ),
          emitter,
          s3Service,
          segmentPusher,
          dataSegmentKiller,
          segmentAnnouncer,
          newSegmentServerView,
          getConglomerate(),
          getJsonMapper()
      );
    }
  }

  public void initializeCuratorFramework() throws IOException
  {
    if (curatorFramework == null) {
      final CuratorConfig curatorConfig = configFactory.build(CuratorConfig.class);
      curatorFramework = Initialization.makeCuratorFrameworkClient(
          curatorConfig,
          lifecycle
      );
    }
  }

  public void initializeServiceDiscovery() throws Exception
  {
    if (serviceDiscovery == null) {
      final ServiceDiscoveryConfig config = configFactory.build(ServiceDiscoveryConfig.class);
      this.serviceDiscovery = Initialization.makeServiceDiscoveryClient(
          curatorFramework,
          config,
          lifecycle
      );
    }
  }

  public void initializeCoordinatorServiceProvider()
  {
    if (coordinatorServiceProvider == null) {
      this.coordinatorServiceProvider = Initialization.makeServiceProvider(
          workerConfig.getMasterService(),
          serviceDiscovery,
          lifecycle
      );
    }
  }

  public void initializeCuratorCoordinator()
  {
    if (workerCuratorCoordinator == null) {
      workerCuratorCoordinator = new WorkerCuratorCoordinator(
          getJsonMapper(),
          configFactory.build(IndexerZkConfig.class),
          curatorFramework,
          new Worker(workerConfig)
      );
      lifecycle.addManagedInstance(workerCuratorCoordinator);
    }
  }

  private void initializeNewSegmentServerView()
  {
    if (newSegmentServerView == null) {
      final MutableServerView view = new OnlyNewSegmentWatcherServerView();
      final ClientInventoryManager clientInventoryManager = new ClientInventoryManager(
          getConfigFactory().build(ClientConfig.class),
          getPhoneBook(),
          view
      );
      lifecycle.addManagedInstance(clientInventoryManager);

      this.newSegmentServerView = view;
    }
  }

  public void initializeWorkerTaskMonitor()
  {
    if (workerTaskMonitor == null) {
      final ExecutorService workerExec = Executors.newFixedThreadPool(workerConfig.getNumThreads());
      final PathChildrenCache pathChildrenCache = new PathChildrenCache(
          curatorFramework,
          workerCuratorCoordinator.getTaskPathForWorker(),
          false
      );
      workerTaskMonitor = new WorkerTaskMonitor(
          pathChildrenCache,
          curatorFramework,
          workerCuratorCoordinator,
          taskToolboxFactory,
          workerExec
      );
      lifecycle.addManagedInstance(workerTaskMonitor);
    }
  }

  public static class Builder
  {
    private ObjectMapper jsonMapper = null;
    private ObjectMapper smileMapper = null;
    private Lifecycle lifecycle = null;
    private Properties props = null;
    private ConfigurationObjectFactory configFactory = null;

    public Builder withMapper(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public Builder withLifecycle(Lifecycle lifecycle)
    {
      this.lifecycle = lifecycle;
      return this;
    }

    public Builder withProps(Properties props)
    {
      this.props = props;
      return this;
    }

    public Builder withConfigFactory(ConfigurationObjectFactory configFactory)
    {
      this.configFactory = configFactory;
      return this;
    }

    public WorkerNode build()
    {
      if (jsonMapper == null && smileMapper == null) {
        jsonMapper = new DefaultObjectMapper();
        smileMapper = new DefaultObjectMapper(new SmileFactory());
        smileMapper.getJsonFactory().setCodec(smileMapper);
      } else if (jsonMapper == null || smileMapper == null) {
        throw new ISE(
            "Only jsonMapper[%s] or smileMapper[%s] was set, must set neither or both.",
            jsonMapper,
            smileMapper
        );
      }

      if (lifecycle == null) {
        lifecycle = new Lifecycle();
      }

      if (props == null) {
        props = Initialization.loadProperties();
      }

      if (configFactory == null) {
        configFactory = Config.createFactory(props);
      }

      return new WorkerNode(props, lifecycle, jsonMapper, smileMapper, configFactory);
    }
  }
}
