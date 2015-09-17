/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.dataflow.geode;

import java.io.File;
import java.util.Collections;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

import org.springframework.cloud.dataflow.core.ModuleDeploymentId;
import org.springframework.cloud.dataflow.core.ModuleDeploymentRequest;
import org.springframework.cloud.dataflow.module.deployer.ModuleDeployer;
import org.springframework.cloud.dataflow.module.deployer.local.LocalModuleDeployer;
import org.springframework.cloud.stream.module.launcher.ModuleLauncher;
import org.springframework.cloud.stream.module.resolver.AetherModuleResolver;
import org.springframework.cloud.stream.module.resolver.ModuleResolver;

/**
 * @author Patrick Peralta
 */
public class ModuleDeploymentCacheListener implements CacheListener<ModuleDeploymentId, ModuleDeploymentRequest> {

	private final ModuleDeployer moduleDeployer;

	public ModuleDeploymentCacheListener() {
		File localRepo = new File(System.getProperty("user.home") +
				File.separator + ".m2" + File.separator + "repository");
		ModuleResolver moduleResolver = new AetherModuleResolver(localRepo, Collections.emptyMap());
		ModuleLauncher moduleLauncher = new ModuleLauncher(moduleResolver);
		moduleDeployer = new LocalModuleDeployer(moduleLauncher);
	}

	@Override
	public void afterCreate(EntryEvent<ModuleDeploymentId, ModuleDeploymentRequest> entryEvent) {
		moduleDeployer.deploy(entryEvent.getNewValue());
	}

	@Override
	public void afterUpdate(EntryEvent<ModuleDeploymentId, ModuleDeploymentRequest> entryEvent) {
	}

	@Override
	public void afterInvalidate(EntryEvent<ModuleDeploymentId, ModuleDeploymentRequest> entryEvent) {
		moduleDeployer.undeploy(entryEvent.getKey());
	}

	@Override
	public void afterDestroy(EntryEvent<ModuleDeploymentId, ModuleDeploymentRequest> entryEvent) {
		moduleDeployer.undeploy(entryEvent.getKey());
	}

	@Override
	public void afterRegionInvalidate(RegionEvent<ModuleDeploymentId, ModuleDeploymentRequest> regionEvent) {
	}

	@Override
	public void afterRegionDestroy(RegionEvent<ModuleDeploymentId, ModuleDeploymentRequest> regionEvent) {
	}

	@Override
	public void afterRegionClear(RegionEvent<ModuleDeploymentId, ModuleDeploymentRequest> regionEvent) {
	}

	@Override
	public void afterRegionCreate(RegionEvent<ModuleDeploymentId, ModuleDeploymentRequest> regionEvent) {
	}

	@Override
	public void afterRegionLive(RegionEvent<ModuleDeploymentId, ModuleDeploymentRequest> regionEvent) {
	}

	@Override
	public void close() {
	}
}
