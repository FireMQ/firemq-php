<?php

/**
 * Publish/subscribe: a fanout exchange copies every message to every bound queue.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;
use FireMQ\QueueOptions;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareExchange('logs.php', 'fanout');

// A queue with a server-generated name that is deleted when this connection closes.
$queue = $client->declareQueue('', QueueOptions::temporary());
$client->bindQueue($queue->name, 'logs.php');

foreach (['info: starting', 'warning: disk at 80%', 'info: ready'] as $line) {
    $client->publish('logs.php', '', $line);
}

$subscription = $client->subscribe($queue->name, ['no_ack' => true]);
for ($i = 0; $i < 3; $i++) {
    $message = $subscription->next(5.0);
    if ($message === null) {
        break;
    }
    echo " [x] {$message->body}\n";
}

$client->close();
