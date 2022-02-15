/**
 * Copyright © 2016-2022 The Thingsboard Authors
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
package org.thingsboard.server.service.device;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.device.profile.Lwm2mDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.controller.AbstractControllerTest;
import org.thingsboard.server.service.importing.BulkImportColumnType;
import org.thingsboard.server.service.importing.BulkImportRequest;
import org.thingsboard.server.service.importing.BulkImportResult;
import org.thingsboard.server.utils.CsvUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.thingsboard.server.service.importing.BulkImportColumnType.LWM2M_CLIENT_ENDPOINT;
import static org.thingsboard.server.service.importing.BulkImportColumnType.NAME;
import static org.thingsboard.server.service.importing.BulkImportColumnType.TYPE;

public abstract class BaseDeviceBulkImportTest extends AbstractControllerTest {

    @Before
    public void setUp() throws Exception {
        loginTenantAdmin();
    }

    @Test
    public void test() {
        System.out.println();
    }

    @Ignore
    public void testBulkImportOfLwm2mDevices_defaultLwm2mProfileConfig() throws Exception {
        BulkImportRequest bulkImportRequest = toBulkImportRequest(List.of(
                Map.of(TYPE, "100",
                        NAME, "1",
                        LWM2M_CLIENT_ENDPOINT, "endpoint1")
        ));

        BulkImportResult<Device> result = requestBulkImport(bulkImportRequest);

        assertEquals(0, result.getErrors().get());
        assertTrue(CollectionUtils.isEmpty(result.getErrorsList()));
        assertEquals(1, result.getCreated().get());

        PageData<DeviceProfile> deviceProfiles = readResponse(doGet("/api/deviceProfiles?page=0&pageSize=1000&textSearch=100"), new TypeReference<>() {});
        assertEquals(1, deviceProfiles.getTotalElements());
        DeviceProfile lwm2mDeviceProfile = deviceProfiles.getData().get(0);
        assertEquals(DeviceTransportType.LWM2M, lwm2mDeviceProfile.getTransportType());
        assertEquals(lwm2mDeviceProfile.getName(), "100");
        Lwm2mDeviceProfileTransportConfiguration transportConfiguration = (Lwm2mDeviceProfileTransportConfiguration) lwm2mDeviceProfile.getProfileData().getTransportConfiguration();
        assertNotNull(transportConfiguration.getBootstrap());
        assertNotNull(transportConfiguration.getObserveAttr());
        assertNotNull(transportConfiguration.getObserveAttr().getObserve());
        assertNotNull(transportConfiguration.getObserveAttr().getAttribute());
        assertNotNull(transportConfiguration.getObserveAttr().getAttributeLwm2m());
        assertNotNull(transportConfiguration.getObserveAttr().getTelemetry());
        assertNotNull(transportConfiguration.getObserveAttr().getKeyName());
        assertNotNull(transportConfiguration.getClientLwM2mSettings());
        assertNotNull(transportConfiguration.getClientLwM2mSettings().getPowerMode());
        assertNotNull(transportConfiguration.getClientLwM2mSettings().getClientOnlyObserveAfterConnect());
        assertNotNull(transportConfiguration.getClientLwM2mSettings().getSwUpdateStrategy());
        assertNotNull(transportConfiguration.getClientLwM2mSettings().getFwUpdateStrategy());
    }

    protected BulkImportRequest toBulkImportRequest(List<Map<BulkImportColumnType, Object>> values) throws IOException {
        BulkImportRequest bulkImportRequest = new BulkImportRequest();

        BulkImportRequest.Mapping mapping = new BulkImportRequest.Mapping();
        mapping.setColumns(values.get(0).keySet().stream()
                .map(columnType -> {
                    BulkImportRequest.ColumnMapping columnMapping = new BulkImportRequest.ColumnMapping();
                    columnMapping.setType(columnType);
                    return columnMapping;
                })
                .collect(Collectors.toList()));
        mapping.setDelimiter(',');
        mapping.setUpdate(false);
        mapping.setHeader(false);
        bulkImportRequest.setMapping(mapping);

        List<List<String>> records = values.stream()
                .map(record -> record.values().stream().map(Object::toString).collect(Collectors.toList()))
                .collect(Collectors.toList());
        String csv = CsvUtils.toCsv(records, mapping.getDelimiter());
        bulkImportRequest.setFile(csv);

        return bulkImportRequest;
    }

    protected BulkImportResult<Device> requestBulkImport(BulkImportRequest bulkImportRequest) throws Exception {
        return readResponse(doPost("/api/device/bulk_import", bulkImportRequest)
                .andExpect(status().isOk()), new TypeReference<BulkImportResult<Device>>() {});
    }

}
