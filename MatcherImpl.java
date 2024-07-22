package com.real.matcher;

import com.real.matcher.model.ActorsAndDirectors;
import com.real.matcher.model.ExternalDB;
import com.real.matcher.model.Movie;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class MatcherImpl implements Matcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);
  List<Movie> movies;
  List<ActorsAndDirectors> actorsAndDirectors;

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("importing database");
    movies = loadMovies(movieDb);
    actorsAndDirectors = loadActorsAndDirectors(actorAndDirectorDb);
    LOGGER.info("database imported");
  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    List<ExternalDB> externalDBS = loadExternalDB(externalDb);
    List<IdMapping> idMappings = new ArrayList<>();
    externalDBS.parallelStream().filter(e -> "Movie".equalsIgnoreCase(e.getMediatype())).forEach(e -> {
      //LOGGER.info("ExternalDB - Movie : {}", e.getTitle());
      movies.parallelStream().filter(m -> e.getTitle().equalsIgnoreCase(m.getTitle())).forEach(m -> {
        //LOGGER.info("Movie title matched : {}, Id:{}", m.getTitle(), m.getId());
        if(actorsAndDirectors.parallelStream().filter(a -> m.getId().equals(a.getMovie_id()) && e.getActors().contains(a.getName())).findFirst().isPresent()){
          //LOGGER.info("Movie title and actors matched, {} , {}, Movie-ID : {}, Media-ID : {}", m.getTitle(), e.getActors(), m.getId(), e.getMediaid());
          IdMapping idMapping = new IdMapping(Integer.valueOf(m.getId()), e.getMediaid());
          if(!idMappings.contains(idMapping)){
            idMappings.add(idMapping);
          }
        }
      });
    });

    return idMappings;
  }

  private List<ExternalDB> loadExternalDB(CsvStream externalDbStream) {
    List<ExternalDB> externalDBS = new ArrayList<>();
    List<List<String>> values = externalDbStream.getDataRows()
            .map((line) -> Arrays.asList(line.split(",")))
            .collect(Collectors.toList());
    List<String> headers = Arrays.stream(externalDbStream.getHeaderRow().split(",")).map(m -> m.toLowerCase()).collect(Collectors.toList());
    values.stream().forEach((l)-> {
      ExternalDB externalDB = new ExternalDB();

      for(int i=0; i < headers.size(); i++){
        try {
          BeanUtils.setProperty(externalDB, headers.get(i), l.get(i));
        } catch (IllegalAccessException e) {
          LOGGER.error("Error : {}", e);
        } catch (InvocationTargetException e) {
          LOGGER.error("Error : {}", e);
        }
      }
      externalDBS.add(externalDB);
    });
    return externalDBS;
  }

  private List<Movie> loadMovies(CsvStream movieDb){
    List<Movie> movies = new ArrayList<>();
    List<List<String>> values = movieDb.getDataRows()
            .map((line) -> Arrays.asList(line.split(",")))
            .collect(Collectors.toList());
    List<String> headers = Arrays.stream(movieDb.getHeaderRow().split(",")).collect(Collectors.toList());
    values.stream().forEach((l)-> {
      Movie m = new Movie();
      for(int i=0; i < headers.size(); i++){
        try {
          BeanUtils.setProperty(m, headers.get(i), l.get(i));
        } catch (IllegalAccessException e) {
          LOGGER.error("Error : {}", e);
        } catch (InvocationTargetException e) {
          LOGGER.error("Error : {}", e);
        }
      }
      movies.add(m);
    });
    return movies;
  }

  private List<ActorsAndDirectors> loadActorsAndDirectors(CsvStream actorAndDirectorDb){
    List<ActorsAndDirectors> actorsAndDirectors = new ArrayList<>();
    List<List<String>> values = actorAndDirectorDb.getDataRows()
            .map((line) -> Arrays.asList(line.split(",")))
            .collect(Collectors.toList());
    List<String> headers = Arrays.stream(actorAndDirectorDb.getHeaderRow().split(",")).collect(Collectors.toList());
    values.stream().forEach((l)-> {
      ActorsAndDirectors m = new ActorsAndDirectors();
      for(int i=0; i < headers.size(); i++){
        try {
          BeanUtils.setProperty(m, headers.get(i), l.get(i));
        } catch (IllegalAccessException e) {
          LOGGER.error("Error : {}", e);
        } catch (InvocationTargetException e) {
          LOGGER.error("Error : {}", e);
        }
      }
      actorsAndDirectors.add(m);
    });
    return actorsAndDirectors;
  }


}
