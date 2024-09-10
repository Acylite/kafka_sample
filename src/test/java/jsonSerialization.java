import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;


import sample.data.models.BU.RDTO;
import java.util.stream.Collectors;

public class jsonSerialization {
    
    @Test
    public void testMessageIdOptions() {
        String json ="{\"id\":\"66d04ed2f61afe3042098259\",\"name\":\"Man Utd vs Liverpool\",\"openTime\":1724932800000,\"closeTime\":1725202799000,\"archiveTime\":1725533999000,\"prize\":{\"value\":\"£5,000*\",\"exceptions\":[{\"regionIds\":[\"39\"],\"value\":\"€5,000*\"}]},\"questions\":null,\"event\":{\"id\":\"1250612\",\"name\":\"Manchester United Vs Liverpool\",\"startTime\":1725202800000,\"competition\":{\"id\":\"65\",\"name\":\"Premier League\"}},\"game\":{\"id\":\"66b343f0531a6863cdc80163\"}}";
        RDTO roundsObject = new RDTO(json);
        System.out.println(roundsObject.prize.exceptions.stream().map(RDTO.ExceptionsDTO::toTableRow).collect(Collectors.toList()));
    }
} 
