package sample.data.models;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import sample.data.models.surveyquestion.OptionTuple;

import java.io.Serializable;

public class OptionsDTO implements Serializable {
    public Integer option_id;
    public String option;

    public static TableSchema BqTableSchema = new TableSchema().setFields(
        ImmutableList.of(
            new TableFieldSchema().setName("option_id").setType("INTEGER").setMode("NULLABLE"),
            new TableFieldSchema().setName("option").setType("STRING").setMode("NULLABLE")
        )
    );

    // Constructor of OneQuestionSurveyQuestion class
    @JsonIgnore
    public OptionsDTO(OptionTuple<Integer, String> element) {
        this.option = element.getOption();
        this.option_id = element.getOptionId();
    }

    
    @JsonIgnore
    public TableRow toTableRow() {
        TableRow row = new TableRow();
        row.set("option_id", option_id);
        row.set("option", option);
        return row;
    }
}