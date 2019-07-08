package io.vertx.guides.wiki.http;

import java.util.Date;

import com.github.rjeschke.txtmark.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;
import io.vertx.guides.wiki.database.WikiDatabaseService;

public class HttpServerVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n\nFeel-free to write in Markdown!\n";

  private String wikiDBQueue = "wikidb.queue";

  private FreeMarkerTemplateEngine templateEngine;

  private WikiDatabaseService dbService;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    wikiDBQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");
    dbService = WikiDatabaseService.createProxy(vertx, wikiDBQueue);

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
    dbService.fetchAllPages(reply -> {
      if (reply.succeeded()) {
        context.put("title", "wiki home");
        context.put("pages", reply.result().getList());
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
    dbService.fetchPage(requestedPage, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result();

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
    Handler<AsyncResult<Void>> handler = reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/wiki/" + title);
      } else {
        context.fail(reply.cause());
      }
    };

    String markdown = context.request().getParam("markdown");
    if ("yes".equals(context.request().getParam("newPage"))) {
      dbService.createPage(title, markdown, handler);
    } else {
      dbService.savePage(Integer.valueOf(context.request().getParam("id")), markdown, handler);
    }
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
    dbService.deletePage(Integer.valueOf(context.request().getParam("id")), reply -> {
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
