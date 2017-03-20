/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.hdl.tensorflow.yarn.app;

import com.github.hdl.tensorflow.bridge.TFServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class TFServerLauncher {
  private static final Log LOG = LogFactory.getLog(TFServerLauncher.class);

  public static final String OPT_CS = "cs";
  public static final String OPT_TI = "ti";
  public static final String OPT_JN = "jn";


  private String clusterSpecString = null;
  private Map<String, List<String>> cluster = null;
  private String jobName = null;
  private int taskIndex = -1;

  // Command line options
  private Options opts;

  public static void main(String[] args) {
    LOG.info("start container");
    TFServerLauncher server = new TFServerLauncher();
    try {
      try {
        if (!server.init(args)) {
          LOG.info("init failed!");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (ParseException e) {
      LOG.info("parse failed");
      e.printStackTrace();
    }
    server.startTFServer();
  }


  public TFServerLauncher() {
    opts = new Options();
    opts.addOption(OPT_CS, true, "tf server cluster spec");
    opts.addOption(OPT_JN, true, "tf job name");
    opts.addOption(OPT_TI, true, "tf task index");
  }

  public boolean init(String[] args) throws ParseException, IOException {

    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for tf server to initialize");
    }

    if (!cliParser.hasOption(OPT_CS) || !cliParser.hasOption(OPT_JN) || !cliParser.hasOption(OPT_TI)) {
      LOG.error("invalid args for tf server!");
      return false;
    }

    clusterSpecString = ClusterSpec.decodeJsonString(cliParser.getOptionValue(OPT_CS));
    jobName = cliParser.getOptionValue(OPT_JN);
    taskIndex = Integer.parseInt(cliParser.getOptionValue(OPT_TI));
    LOG.info("cs: " + clusterSpecString + "; + jn: " + jobName + "; ti: " + taskIndex);
    cluster = ClusterSpec.toClusterMapFromJsonString(clusterSpecString);
    return true;
  }

  public void startTFServer() {
    LOG.info("Launch a new tensorflow " + jobName + taskIndex);

    TFServer server = new TFServer(cluster, jobName, taskIndex);
    server.start();
    server.join();
    LOG.info("Ternsorflow " + jobName + taskIndex + "stopped!");
  }
}
