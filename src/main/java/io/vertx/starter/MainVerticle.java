package io.vertx.starter;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.github.rjeschke.txtmark.Processor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.common.template.TemplateEngine;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

  private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n";

  private JDBCClient dbClient;
  private TemplateEngine tempalteEngine;

  @Override
  public void start(Future<Void> startFuture) {
    Future<String> dbVerticleDeployment = Future.future();
    vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment.completer());

    dbVerticleDeployment.compose(id -> {
      Future<String> httpVerticleDeployment = Future.future();
      vertx.deployVerticle("io.vertx.guides.wiki.HttpServerVerticle", new DeploymentOptions().setInstances(2), httpVerticleDeployment.completer())
    }).setHandler(ar -> {
      if (ar.succeeded()) {
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
  }

  private Future<Void> prepareDatabase() {
    Future<Void> future = Future.future();
    dbClient = JDBCClient.createShared(vertx, new JsonObject().put("url", "jdbc:hsqldb:file:db/wiki")
        .put("driver_class", "org.hsqldb.jdbcDriver").put("max_pool_size", 30));

    dbClient.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.info("could not open a database connection", ar.cause());
        future.fail(ar.cause());
      } else {
        SQLConnection connection = ar.result();
        connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
          connection.close();
          if (create.failed()) {
            LOGGER.info("database preparation error", create.cause());
            future.fail(create.cause());
          } else {
            future.complete();
          }
        });
      }
    });
    return future;
  }

  private Future<Void> startHttpServer() {
    Future<Void> future = Future.future();
    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeleteHandler);

    tempalteEngine = FreeMarkerTemplateEngine.create(vertx);

    server.requestHandler(router).listen(8080, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("http server running on port 8080");
        future.complete();
      } else {
        LOGGER.error("could not start a http server", ar.cause());
        future.fail(ar.cause());
      }
    });
    return future;
  }

  private void indexHandler(RoutingContext context) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.query(SQL_ALL_PAGES, res -> {
          connection.close();

          if (res.succeeded()) {
            List<String> pages = res.result().getResults().stream().map(json -> json.getString(0)).sorted()
                .collect(Collectors.toList());

            context.put("title", "Wiki home");
            context.put("pages", pages);
            tempalteEngine.render(context.data(), "templates/index.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause());
              }
            });
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    String page = context.request().getParam("page");

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
          if (fetch.succeeded()) {
            JsonArray row = fetch.result().getResults().stream().findFirst()
                .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
            Integer id = row.getInteger(0);
            String rawContent = row.getString(1);

            context.put("title", page);
            context.put("id", id);
            context.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
            context.put("content", Processor.process(rawContent));
            context.put("timestamp", new Date().toString());

            tempalteEngine.render(context.data(), "templates/page.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                context.fail(ar.cause());
              }
            });
          } else {
            context.fail(fetch.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if ((pageName == null) || (pageName.isEmpty())) {
      location = "/";
    }
    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    String title = context.request().getParam("title");
    String markdown = context.request().getParam("markdown");
    boolean newPage = "yes".equals(context.request().getParam("newPage"));

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
        JsonArray params = new JsonArray();
        if (newPage) {
          params.add(title).add(markdown);
        } else {
          params.add(markdown).add(id);
        }
        connection.updateWithParams(sql, params, res -> {
          if (res.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/wiki/" + title);
            context.response().end();
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }

  private void pageDeleteHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
          if (res.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/");
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }
}

class HttpServerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n";

  private String wikiDBQueue = "wikidb.queue";

  private FreeMarkerTemplateEngine templateEngine;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    wikiDBQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeleteHandler);

    templateEngine = FreeMarkerTemplateEngine.create(vertx);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server.requestHandler(router).listen(portNumber, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("http server running on port " + portNumber);
        startFuture.complete();
      } else {
        LOGGER.error("could not start a http server", ar.cause());
        startFuture.fail(ar.cause());
      }
    });
  }

  private void indexHandler(RoutingContext context) {
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "all-pages");

    vertx.eventBus().send(wikiDBQueue, new JsonObject(), options, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result().body();
        context.put("title", "wiki home");
        context.put("pages", body.getJsonArray("pages").getList());
        templateEngine.render(context.data(), "templates/index.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    String requestedPage = context.request().getParam("page");
    JsonObject request = new JsonObject().put("page", requestedPage);

    DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-pages");
    vertx.eventBus().send(wikiDBQueue, request, options, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result().body();

        boolean found = body.getBoolean("found");
        String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);
        context.put("title", requestedPage);
        context.put("id", body.getInteger("id", -1));
        context.put("newPage", found ? "no" : "yes");
        context.put("rawContent", rawContent);
        context.put("content", Processor.process(rawContent));
        context.put("timestamp", new Date().toString());
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageUpdateHandler(RoutingContext context) {
    String title = context.request().getParam("title");
    JsonObject request = new JsonObject().put("id", context.request().getParam("id")).put("title", title)
        .put("markdown", context.request().getParam("markdown"));

    DeliveryOptions options = new DeliveryOptions();
    if ("yes".equals(context.request().getParam("newPage"))) {
      options.addHeader("action", "create-page");
    } else {
      options.addHeader("action", "save-page");
    }

    vertx.eventBus().send(wikiDBQueue, request, options, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/wiki/" + title);
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;

    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }

    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageDeleteHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    JsonObject request = new JsonObject().put("id", id);
    DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-pages");
    vertx.eventBus().send(wikiDBQueue, request, options, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/");
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }
}

class WikiDatabaseVerticle extends AbstractVerticle {

  public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
  public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
  public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
  public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueryies.resource.file";

  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

  private JDBCClient dbClient;

  private enum SqlQuery {
    CREATE_PAGES_TABLE, ALL_PAGES, GET_PAGES, CREATE_PAGE, SAVE_PAGE, DELETE_PAGE
  }

  private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();

  private void loadSqlQueries() throws IOException {
    String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
    InputStream queriesInputStream;
    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(queriesInputStream);
    queriesInputStream.close();

    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-tables"));
    sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
    sqlQueries.put(SqlQuery.GET_PAGES, queriesProps.getProperty("get-page"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    loadSqlQueries();

    dbClient = JDBCClient.createShared(vertx,
        new JsonObject().put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
            .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
            .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

    dbClient.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("could not open a database connection", ar.cause());
        startFuture.fail(ar.cause());
      } else {
        SQLConnection connection = ar.result();
        connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
          if (create.failed()) {
            LOGGER.error("database preparation error", create.cause());
            startFuture.fail(create.cause());
          } else {
            vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage);
            startFuture.complete();
          }
        });
      }
    });
  }

  public enum ErrorCodes {
    NO_ACTION_SPECIFIED, BAD_ACTION, DB_ERROR
  }

  public void onMessage(Message<JsonObject> message) {
    if (!message.headers().contains("action")) {
      LOGGER.error("no action header specified for message with headers {} and body {}", message.headers(),
          message.body().encodePrettily());
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "no action header specified");
      return;
    }

    String action = message.headers().get("action");

    switch (action) {
    case "all-pages":
      fetchAllPages(message);
      break;

    case "get-page":
      fetchPage(message);
      break;

    case "create-page":
      createPage(message);
      break;

    case "save-page":
      savePage(message);
      break;

    case "delete-page":
      deletePage(message);
      break;

    default:
      message.fail(ErrorCodes.BAD_ACTION.ordinal(), "bad action: " + action);
    }
  }

  private void fetchAllPages(Message<JsonObject> message) {
    dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
      if (res.succeeded()) {
        List<String> pages = res.result().getResults().stream().map(json -> json.getString(0)).sorted()
            .collect(Collectors.toList());
        message.reply(new JsonObject().put("pages", new JsonArray(pages)));
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void fetchPage(Message<JsonObject> message) {
    String requestedPage = message.body().getString("page");
    JsonArray params = new JsonArray().add(requestedPage);

    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGES), params, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = fetch.result();
        if (resultSet.getNumRows() == 0) {
          response.put("found", false);
        } else {
          response.put("found", true);
          JsonArray row = resultSet.getResults().get(0);
          response.put("id", row.getInteger(0));
          response.put("rawContent", row.getString(1));
        }
        message.reply(response);
      } else {
        reportQueryError(message, fetch.cause());
      }
    });
  }

  private void createPage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray data = new JsonArray().add(request.getString("title")).add(request.getString("markdown"));

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
      if (res.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void savePage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray data = new JsonArray().add(request.getString("markdown")).add(request.getString("id"));

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
      if (res.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void deletePage(Message<JsonObject> message) {
    JsonArray data = new JsonArray().add(message.body().getString("id"));

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
      if (res.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, res.cause());
      }
    });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database query error", cause);
    message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }
}
