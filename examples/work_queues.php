<?php

/**
 * Work queues: share tasks between workers, one unacknowledged task per worker at a time.
 *
 * Run several copies of the consume() part in separate processes to spread the work.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;
use FireMQ\Message;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareQueue('task_queue.php');

for ($n = 1; $n <= 3; $n++) {
    // Persistent by default: returns once the broker has written the task to disk.
    $client->publish('', 'task_queue.php', "task $n");
    echo " [x] Sent 'task $n'\n";
}

$done = 0;
$client->consume('task_queue.php', function (Message $task) use (&$done): bool {
    echo " [x] Done {$task->body}\n";
    $done++;

    // The task is acknowledged when the handler returns. Return false to stop consuming.
    return $done < 3;
}, ['prefetch' => 1]);

$client->close();
