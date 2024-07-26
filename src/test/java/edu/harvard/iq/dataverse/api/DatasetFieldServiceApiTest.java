package edu.harvard.iq.dataverse.api;

import edu.harvard.iq.dataverse.ControlledVocabularyValue;
import edu.harvard.iq.dataverse.ControlledVocabularyValueServiceBean;
import edu.harvard.iq.dataverse.DatasetFieldServiceBean;
import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.MetadataBlock;
import edu.harvard.iq.dataverse.MetadataBlockServiceBean;
import edu.harvard.iq.dataverse.actionlogging.ActionLogRecord;
import edu.harvard.iq.dataverse.actionlogging.ActionLogServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.persistence.EntityManager;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DatasetFieldServiceApiTest {


    @Spy
    private ActionLogServiceBean actionLogSvc;

    @Mock
    private MetadataBlockServiceBean metadataBlockService;

    @Mock
    private DatasetFieldServiceBean datasetFieldService;

    @Mock
    private ControlledVocabularyValueServiceBean controlledVocabularyValueService;

    @Mock
    private EntityManager em;

    @InjectMocks
    private DatasetFieldServiceApi api;

    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        Field entityManagerField = ActionLogServiceBean.class.getDeclaredField("em");
        entityManagerField.setAccessible(true);
        entityManagerField.set(actionLogSvc, em);
    }


    @Test
    public void testArrayIndexOutOfBoundMessageBundle() {
        List<String> arguments = new ArrayList<>();
        arguments.add("DATASETFIELD");
        arguments.add(String.valueOf(5));
        arguments.add("watermark");
        arguments.add(String.valueOf(4 + 1));

        String bundle = "api.admin.datasetfield.load.ArrayIndexOutOfBoundMessage";
        String message = BundleUtil.getStringFromBundle(bundle, arguments);
        assertEquals(
            "Error parsing metadata block in DATASETFIELD part, line #5: missing 'watermark' column (#5)",
            message
        );
    }

    @Test
    public void testGeneralErrorMessageBundle() {
        List<String> arguments = new ArrayList<>();
        arguments.add("DATASETFIELD");
        arguments.add(String.valueOf(5));
        arguments.add("some error message");
        String bundle = "api.admin.datasetfield.load.GeneralErrorMessage";
        String message = BundleUtil.getStringFromBundle(bundle, arguments);
        assertEquals(
            "Error parsing metadata block in DATASETFIELD part, line #5: some error message",
            message
        );
    }

    @Test
    public void testGetArrayIndexOutOfBoundMessage() {
        DatasetFieldServiceApi api = new DatasetFieldServiceApi();
        String message = api.getArrayIndexOutOfBoundMessage(DatasetFieldServiceApi.HeaderType.DATASETFIELD, 5, 4);
        assertEquals(
            "Error parsing metadata block in DATASETFIELD part, line #5: missing 'watermark' column (#5)",
            message
        );
    }

    @Test
    public void testGetMessage() {
        DatasetFieldServiceApi api = new DatasetFieldServiceApi();
        String message = api.getMessage("api.admin.datasetfield.load.GeneralErrorMessage",
            DatasetFieldServiceApi.HeaderType.DATASETFIELD.name(), String.valueOf(5), "some error");
        assertEquals(
            "Error parsing metadata block in DATASETFIELD part, line #5: some error",
            message
        );
    }

    @Test
    public void testRenameMetadataSuccess() {
        final String jsonBody = """
            {
              "old": "before",
              "new": "after"
            }
            """;
        MetadataBlock metadataBlock = new MetadataBlock();
        metadataBlock.setName("before");
        when(metadataBlockService.findByName("before")).thenReturn(metadataBlock);
        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameMetadata(jsonBody);

        assertEquals(200, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();

        final String headerName = "renamed "
            + DatasetFieldServiceApi.HeaderType.METADATABLOCK.name();
        assertTrue(jsonResponse.getJsonObject("data").containsKey(headerName));

        String before = parseJsonData(jsonResponse, headerName, "oldName");
        assertEquals("before", before);

        String after = parseJsonData(jsonResponse, headerName, "newName");
        assertEquals("after", after);

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.OK, capturedLogRecord.getActionResult());

        assertEquals("rename " + DatasetFieldServiceApi.HeaderType.METADATABLOCK.name() +
            " from \"before\" to \"after\"", capturedLogRecord.getInfo());
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameMetadataNameNotFound() {
        final String jsonBody = """
            {
              "old": "notThere",
              "new": "after"
            }
            """;

        when(metadataBlockService.findByName("notThere")).thenReturn(null);
        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameMetadata(jsonBody);

        assertEquals(404, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();

        assertTrue(jsonResponse.getString("message").contains(
            DatasetFieldServiceApi.HeaderType.METADATABLOCK.name() +
                " with value \"notThere\" not found"));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.BadRequest, capturedLogRecord.getActionResult());

        assertTrue(capturedLogRecord.getInfo().contains("rename " + DatasetFieldServiceApi.HeaderType.METADATABLOCK.name() +
            " from \"notThere\" to \"after\""));
        assertTrue(capturedLogRecord.getInfo().contains(DatasetFieldServiceApi.HeaderType.METADATABLOCK.name() +
            " with value \"notThere\" not found"));
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameMetadataNewNameExists() {
        final String jsonBody = """
            {
              "old": "before",
              "new": "after"
            }
            """;
        MetadataBlock metadataBlock1 = new MetadataBlock();
        metadataBlock1.setName("before");
        when(metadataBlockService.findByName("before")).thenReturn(metadataBlock1);
        MetadataBlock metadataBlock2 = new MetadataBlock();
        metadataBlock2.setName("after");
        when(metadataBlockService.findByName("after")).thenReturn(metadataBlock2);
        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameMetadata(jsonBody);

        assertEquals(409, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();

        assertTrue(jsonResponse.getString("message").contains(
            "new name \"after\" is already in use"));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.InternalError, capturedLogRecord.getActionResult());

        assertTrue(capturedLogRecord.getInfo().contains("rename " + DatasetFieldServiceApi.HeaderType.METADATABLOCK.name() +
            " from \"before\" to \"after\""));

        assertTrue(capturedLogRecord.getInfo().contains("new name \"after\" is already in use"));
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameMetadataMalformedBody() {
        final String jsonBody = "{}";
        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameMetadata(jsonBody);

        assertEquals(417, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();
        assertTrue(jsonResponse.getString("message").contains(
            "request failed with malformed body"));
        assertTrue(jsonResponse.getString("message").contains(
            "JSONObject[\"old\"] not found."));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.BadRequest, capturedLogRecord.getActionResult());

        assertTrue(capturedLogRecord.getInfo().contains("request failed with malformed body"));
        assertTrue(capturedLogRecord.getInfo().contains("JSONObject[\"old\"] not found."));
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameControlledVocabularyNoIdentifier() {
        final String jsonBody = """
            {
            "oldName": "before",
            "newName": "after",
            "datasetName": "dsName"
            }
            """;
        DatasetFieldType dft = new DatasetFieldType();
        dft.setName("dsName");
        ControlledVocabularyValue cvv = new ControlledVocabularyValue();
        cvv.setStrValue("before");
        cvv.setDatasetFieldType(dft);
        MetadataBlock mdb = new MetadataBlock();
        mdb.setName("mdName");
        dft.setMetadataBlock(mdb);

        when(datasetFieldService.findByName("dsName")).thenReturn(dft);
        when(datasetFieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dft,
            "before", true)).thenReturn(cvv);

        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameControlledVocabulary(jsonBody);
        assertEquals(200, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();

        final String headerName = "renamed "
            + DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY.name();
        assertTrue(jsonResponse.getJsonObject("data").containsKey(headerName));

        assertEquals("before", parseJsonData(jsonResponse, headerName, "oldName"));
        assertEquals("after", parseJsonData(jsonResponse, headerName, "newName"));
        assertEquals("", parseJsonData(jsonResponse, headerName, "oldIdentifier"));
        assertEquals("", parseJsonData(jsonResponse, headerName, "newIdentifier"));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.OK, capturedLogRecord.getActionResult());

        assertEquals("rename " + DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY.name() +
            " identifier from \"\" to \"\" and name from \"before\" to \"after\"", capturedLogRecord.getInfo());
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameControlledVocabularyNewNameExists() {
        final String jsonBody = """
            {
            "oldName": "before",
            "newName": "after",
            "oldIdentifier": "identOld",
            "newIdentifier": "identNew",
            "datasetName": "dsName"
            }
            """;
        DatasetFieldType dft = new DatasetFieldType();
        dft.setName("dsName");
        ControlledVocabularyValue cvv1 = new ControlledVocabularyValue();
        cvv1.setStrValue("before");
        cvv1.setIdentifier("identOld");
        cvv1.setDatasetFieldType(dft);
        ControlledVocabularyValue cvv2 = new ControlledVocabularyValue();
        cvv2.setStrValue("after");
        cvv2.setIdentifier("identNew");
        cvv2.setDatasetFieldType(dft);
        MetadataBlock mdb = new MetadataBlock();
        mdb.setName("mdName");
        dft.setMetadataBlock(mdb);

        when(datasetFieldService.findByName("dsName")).thenReturn(dft);
        when(datasetFieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dft,
            "before", true)).thenReturn(cvv1);
        when(datasetFieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dft,
            "after", true)).thenReturn(cvv2);

        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameControlledVocabulary(jsonBody);
        assertEquals(409, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();
        System.out.println(jsonResponse.getString("message"));

        assertTrue(jsonResponse.getString("message").contains(
            DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY.name() +
            " with identifier \"identNew\" and name \"after\" already exists for dataset \"dsName\""));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.InternalError, capturedLogRecord.getActionResult());

        assertTrue(capturedLogRecord.getInfo().contains("rename " + DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY.name() +
            " identifier from \"identOld\" to \"identNew\" and name from \"before\" to \"after\""));

        assertTrue(capturedLogRecord.getInfo().contains(DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY.name() +
            " with identifier \"identNew\" and name \"after\" already exists for dataset \"dsName\""));

        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    @Test
    public void testRenameDatasetUnmodifiable() {
        final String jsonBody = """
            {
              "old": "before",
              "new": "after"
            }
            """;
        MetadataBlock metadataBlock = new MetadataBlock();
        metadataBlock.setName("citation");
        DatasetFieldType datasetFieldType = new DatasetFieldType();
        datasetFieldType.setName("before");
        datasetFieldType.setMetadataBlock(metadataBlock);
        when(datasetFieldService.findByName("before")).thenReturn(datasetFieldType);
        ArgumentCaptor<ActionLogRecord> logCaptor = ArgumentCaptor.forClass(ActionLogRecord.class);

        Response response = api.renameDataset(jsonBody);

        assertEquals(403, response.getStatus());
        JsonReader jsonReader = Json.createReader(new StringReader(response.getEntity().toString()));
        JsonObject jsonResponse = jsonReader.readObject();

        assertTrue(jsonResponse.getString("message").contains(
            "it is not possible to modify metadata from the set citation"));

        verify(actionLogSvc, times(1)).log(logCaptor.capture());
        ActionLogRecord capturedLogRecord = logCaptor.getValue();

        assertEquals(ActionLogRecord.Result.BadRequest, capturedLogRecord.getActionResult());

        assertTrue(capturedLogRecord.getInfo().contains("rename " + DatasetFieldServiceApi.HeaderType.DATASETFIELD.name() +
            " from \"before\" to \"after\""));
        assertTrue(capturedLogRecord.getInfo().contains(
            "it is not possible to modify metadata from the set citation"));
        assertEquals(ActionLogRecord.ActionType.Admin, capturedLogRecord.getActionType());
    }

    private String parseJsonData(JsonObject jsonResponse, String headerName, String keyName) {
        return jsonResponse
            .getJsonObject("data")
            .getJsonArray(headerName)
            .getJsonObject(0).getString(keyName);
    }
}
