import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ConsumerWorker implements Runnable {

    private final String recordValue;

    @Override
    public void run() {
        log.info("thread:{}\trecord:{}", Thread.currentThread().getName(), recordValue);
    }
}
