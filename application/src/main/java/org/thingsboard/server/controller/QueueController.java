/**
 * Copyright © 2016-2021 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.thingsboard.server.common.data.exception.ThingsboardException;
import org.thingsboard.server.common.data.id.QueueId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.queue.Queue;
import org.thingsboard.server.common.msg.queue.ServiceType;
import org.thingsboard.server.queue.util.TbCoreComponent;
import org.thingsboard.server.service.security.permission.Operation;
import org.thingsboard.server.service.security.permission.Resource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@TbCoreComponent
@RequestMapping("/api")
public class QueueController extends BaseController {

    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/queues", params = {"serviceType"}, method = RequestMethod.GET)
    @ResponseBody
    public List<String> getTenantQueuesNameByServiceType(@RequestParam String serviceType) throws ThingsboardException {
        checkParameter("serviceType", serviceType);
        try {
            ServiceType type = ServiceType.valueOf(serviceType);
            switch (type) {
                case TB_RULE_ENGINE:
                    return queueService.findQueuesByTenantId(getTenantId()).stream().map(Queue::getName).collect(Collectors.toList());
                default:
                    return Collections.emptyList();
            }
        } catch (Exception e) {
            throw handleException(e);
        }
    }

    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/queues", params = {"serviceType", "pageSize", "page"}, method = RequestMethod.GET)
    @ResponseBody
    public PageData<Queue> getTenantQueuesByServiceType(@RequestParam String serviceType,
                                                        @RequestParam int pageSize,
                                                        @RequestParam int page,
                                                        @RequestParam(required = false) String textSearch,
                                                        @RequestParam(required = false) String sortProperty,
                                                        @RequestParam(required = false) String sortOrder) throws ThingsboardException {
        checkParameter("serviceType", serviceType);
        try {
            PageLink pageLink = createPageLink(pageSize, page, textSearch, sortProperty, sortOrder);
            ServiceType type = ServiceType.valueOf(serviceType);
            switch (type) {
                case TB_RULE_ENGINE:
                    return queueService.findQueuesByTenantId(getTenantId(), pageLink);
                default:
                    return new PageData<>();
            }
        } catch (Exception e) {
            throw handleException(e);
        }
    }

    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/queues/{queueId}", method = RequestMethod.GET)
    @ResponseBody
    public Queue getQueueById(@PathVariable("queueId") String queueIdStr) throws ThingsboardException {
        checkParameter("queueId", queueIdStr);
        try {
            QueueId queueId = new QueueId(UUID.fromString(queueIdStr));
            return checkNotNull(queueService.findQueueById(getTenantId(), queueId));
        } catch (
                Exception e) {
            throw handleException(e);
        }

    }

    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/queues", params = {"serviceType"}, method = RequestMethod.POST)
    @ResponseBody
    public Queue saveQueue(@RequestBody Queue queue,
                           @RequestParam String serviceType) throws ThingsboardException {
        checkParameter("serviceType", serviceType);
        try {
            queue.setTenantId(getCurrentUser().getTenantId());

            checkEntity(queue.getId(), queue, Resource.QUEUE);

            ServiceType type = ServiceType.valueOf(serviceType);
            switch (type) {
                case TB_RULE_ENGINE:
                    queue.setTenantId(getTenantId());
                    Queue savedQueue = queueService.saveQueue(queue);
                    checkNotNull(savedQueue);
                    return savedQueue;
                default:
                    return null;
            }
        } catch (Exception e) {
            throw handleException(e);
        }
    }

    @PreAuthorize("hasAnyAuthority('SYS_ADMIN', 'TENANT_ADMIN')")
    @RequestMapping(value = "/tenant/queues/{queueId}", method = RequestMethod.DELETE)
    @ResponseBody
    public void deleteQueue(@PathVariable("queueId") String queueIdStr) throws ThingsboardException {
        checkParameter("queueId", queueIdStr);
        try {
            QueueId queueId = new QueueId(toUUID(queueIdStr));
            checkQueueId(queueId, Operation.DELETE);
            queueService.deleteQueue(getTenantId(), queueId);
        } catch (Exception e) {
            throw handleException(e);
        }
    }
}
