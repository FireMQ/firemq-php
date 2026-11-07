<?php

/**
 * Routing: a direct exchange delivers a message to the queues bound with its routing key.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;
use FireMQ\QueueOptions;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareExchange('direct_logs.php', 'direct');

$queue = $client->declareQueue('', QueueOptions::temporary());
foreach (['warning', 'error'] as $severity) {
    $client->bindQueue($queue->name, 'direct_logs.php', $severity);
}

foreach (['info' => 'all good', 'warning' => 'disk at 80%', 'error' => 'disk full'] as $severity => $text) {
    $client->publish('direct_logs.php', $severity, $text);
}

// Only the warning and the error arrive.
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
