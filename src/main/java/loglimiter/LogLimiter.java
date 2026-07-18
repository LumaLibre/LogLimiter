package loglimiter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Filter;
import java.util.logging.LogRecord;

public final class LogLimiter extends JavaPlugin {

    private static LogLimitFilter log4jFilter = null;
    private static final Object log4jLock = new Object();

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> flushTask;
    private Filter previousJulFilter;

    private volatile long flushIntervalSeconds;

    @Override
    public void onEnable() {
        saveDefaultConfig();

        long resetMillis = readResetMillis();
        double similarityThreshold = readSimilarityThreshold();
        int messagesBeforeBlock = readMessagesBeforeBlock();
        this.flushIntervalSeconds = readFlushIntervalSeconds();

        // Log4j2/SLF4J filter
        synchronized (log4jLock) {
            if (log4jFilter == null) {
                log4jFilter = new LogLimitFilter(resetMillis, similarityThreshold, messagesBeforeBlock);
                installLog4jFilter(log4jFilter);
            } else {
                log4jFilter.activate(resetMillis, similarityThreshold, messagesBeforeBlock);
            }
        }

        // JUL filter
        java.util.logging.Logger julRoot = java.util.logging.Logger.getLogger("");
        previousJulFilter = julRoot.getFilter();
        julRoot.setFilter(new JulLimitFilter(log4jFilter, previousJulFilter));

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LogLimiter-Flush");
            t.setDaemon(true);
            return t;
        });
        scheduleFlush();

        getSLF4JLogger().info(
                "Enabled — reset={}s, flush={}s, similarity={}%, messages-before-block={}",
                resetMillis / 1000L, flushIntervalSeconds,
                Math.round(similarityThreshold * 100), messagesBeforeBlock
        );
    }

    @Override
    public void onDisable() {
        if (flushTask != null) flushTask.cancel(false);
        if (scheduler != null) {
            scheduler.shutdown();
            try { scheduler.awaitTermination(5, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }

        synchronized (log4jLock) {
            if (log4jFilter != null) {
                log4jFilter.flush();
                log4jFilter.clear();
                log4jFilter.deactivate();
            }
        }

        java.util.logging.Logger.getLogger("").setFilter(previousJulFilter);
        getSLF4JLogger().info("Disabled.");
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 1 && args[0].equalsIgnoreCase("reload")) {
            if (!sender.hasPermission("loglimiter.command.reload")) {
                sender.sendMessage("You don't have permission to reload LogLimiter.");
                return true;
            }
            reload();
            sender.sendMessage("LogLimiter configuration reloaded.");
            return true;
        }
        sender.sendMessage("Usage: /" + label + " reload");
        return true;
    }

    private void reload() {
        reloadConfig();

        long resetMillis = readResetMillis();
        double similarityThreshold = readSimilarityThreshold();
        int messagesBeforeBlock = readMessagesBeforeBlock();
        long newFlushInterval = readFlushIntervalSeconds();

        synchronized (log4jLock) {
            if (log4jFilter != null) {
                log4jFilter.reconfigure(resetMillis, similarityThreshold, messagesBeforeBlock);
            }
        }

        // Reschedule the flush task only when the interval actually changed
        if (newFlushInterval != flushIntervalSeconds) {
            this.flushIntervalSeconds = newFlushInterval;
            if (flushTask != null) flushTask.cancel(false);
            scheduleFlush();
        }

        getSLF4JLogger().info(
                "Reloaded — reset={}s, flush={}s, similarity={}%, messages-before-block={}",
                resetMillis / 1000L, flushIntervalSeconds,
                Math.round(similarityThreshold * 100), messagesBeforeBlock
        );
    }

    private void scheduleFlush() {
        final LogLimitFilter filterRef = log4jFilter;
        flushTask = scheduler.scheduleAtFixedRate(
                filterRef::flush,
                flushIntervalSeconds,
                flushIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    private long readResetMillis() {
        return getConfig().getLong("reset-seconds", 60L) * 1000L;
    }

    private long readFlushIntervalSeconds() {
        return getConfig().getLong("flush-interval-seconds", 30L);
    }

    private double readSimilarityThreshold() {
        double percent = getConfig().getDouble("similarity-percent", 100.0);
        if (percent < 0.0) percent = 0.0;
        if (percent > 100.0) percent = 100.0;
        return percent / 100.0;
    }

    private int readMessagesBeforeBlock() {
        int value = getConfig().getInt("messages-before-block", 2);
        return Math.max(1, value);
    }

    private static void installLog4jFilter(LogLimitFilter f) {
        try {
            LoggerContext ctx = ((Logger) LogManager.getRootLogger()).getContext();
            Configuration config = ctx.getConfiguration();
            config.getRootLogger().addFilter(f);
            ctx.updateLoggers();
        } catch (Exception e) {
            LogManager.getLogger(LogLimiter.class)
                    .warn("LogLimiter: could not install Log4j2 filter — JUL filter only. ({})", e.toString());
        }
    }

    private static final class JulLimitFilter implements Filter {

        private final LogLimitFilter limiter;
        private final Filter delegate;

        private final ThreadLocal<Boolean> flushing = ThreadLocal.withInitial(() -> false);

        JulLimitFilter(LogLimitFilter limiter, Filter delegate) {
            this.limiter = limiter;
            this.delegate = delegate;
        }

        @Override
        public boolean isLoggable(LogRecord record) {
            if (flushing.get()) return true;

            if (delegate != null && !delegate.isLoggable(record)) return false;
            Level level = julLevelToLog4j(record.getLevel());
            String message = formatJul(record);

            return limiter.checkAndRecord(record.getLoggerName(), level, message);
        }

        private static Level julLevelToLog4j(java.util.logging.Level julLevel) {
            int v = julLevel.intValue();
            if (v >= java.util.logging.Level.SEVERE.intValue())  return Level.ERROR;
            if (v >= java.util.logging.Level.WARNING.intValue()) return Level.WARN;
            if (v >= java.util.logging.Level.FINE.intValue())    return Level.DEBUG;
            return Level.TRACE;
        }

        private static String formatJul(LogRecord record) {
            String msg = record.getMessage();
            if (msg == null) return "";
            Object[] params = record.getParameters();
            if (params != null && params.length > 0) {
                try { msg = MessageFormat.format(msg, params); }
                catch (Exception ignored) { /* use raw */ }
            }
            return msg;
        }
    }
}
