<?php

/**
 * Topics: route with word patterns; `*` matches one word, `#` matches zero or more.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;
use FireMQ\QueueOptions;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareExchange('topic_logs.php', 'topic');

$queue = $client->declareQueue('', QueueOptions::temporary());
$client->bindQueue($queue->name, 'topic_logs.php', '*.critical');

foreach (['kern.critical', 'kern.info', 'app.critical'] as $key) {
    $client->publish('topic_logs.php', $key, "message for $key");
}

$subscription = $client->subscribe($queue->name);
for ($i = 0; $i < 2; $i++) {
    $message = $subscription->next(5.0);
    if ($message === null) {
        break;
    }
    echo " [x] {$message->routingKey}: {$message->body}\n";
    $message->ack();
}

$client->close();
