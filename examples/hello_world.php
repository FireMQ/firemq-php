<?php

/**
 * Hello World: publish one message and consume it.
 *
 * Start a broker with `firemq`, then run `php examples/hello_world.php`.
 * Set FIREMQ_URL to connect somewhere other than fire://localhost.
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;

$client = Client::connect(getenv('FIREMQ_URL') ?: 'fire://localhost');
$client->declareQueue('hello.php');

// Returns once the broker has confirmed the message.
$client->publish('', 'hello.php', 'Hello World!');
echo " [x] Sent 'Hello World!'\n";

$subscription = $client->subscribe('hello.php');
$message = $subscription->next(5.0);
if ($message !== null) {
    echo " [x] Received '{$message->body}'\n";
    $message->ack();
}

$client->close();
