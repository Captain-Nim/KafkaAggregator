package com.apiexpose.KafkaAggregator.repository;

import com.apiexpose.KafkaAggregator.entity.SensorData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SensorDataRepository extends JpaRepository<SensorData, Long>
{
    //    Find All Sensor Data by Topic Name
    List<SensorData> findByTopicName(String topicName);

    //    Find All Sensor Data by Location
    List<SensorData> findByLocation(String location);

    //    Find All Sensor Data by Location and Topic Name
    List<SensorData> findByTopicNameAndLocation(String topicName, String location);

    //    Find All Sensor Data by Timestamp Range
    @Query("select sd from SensorData sd where sd.timestamp between :startTime and :endTime order by sd.timestamp desc")
    List<SensorData> findByTimestampBetween(
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime
    );

//        Get Latest Sensor Data for each Topic
   @Query("SELECT s FROM SensorData s WHERE s.timestamp = " +
        "(SELECT MAX(s2.timestamp) FROM SensorData s2 WHERE s2.topicName = s.topicName) " +
        "ORDER BY s.topicName")
    List<SensorData> findLatestByEachTopic();

}