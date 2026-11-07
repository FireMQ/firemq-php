<?php

/**
 * RPC: a request carries `reply_to` and `correlation_id`; the server answers there.
 *
 * PHP runs one thing at a time, so this script plays both parts with two connections, step
 * by step. In production the server is a separate worker that runs consume().
 */

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use FireMQ\Client;
use FireMQ\QueueOptions;

$url = getenv('FIREMQ_URL') ?: 'fire://localhost';
$caller = Client::connect($url);
$server = Client::connect($url);
$server->declareQueue('rpc_queue.php');

// The caller listens on a private reply queue and sends a request.
$replies = $caller->declareQueue('', QueueOptions::temporary());
$answers = $caller->subscribe($replies->name);
$correlationId = bin2hex(random_bytes(8));
$caller->publish('', 'rpc_queue.php', '7', [
    'properties' => ['reply_to' => $replies->name, 'correlation_id' => $correlationId],
]);
echo " [x] Requested square(7)\n";

// The server squares the number and replies to `reply_to` with the same correlation id.
$request = $server->get('rpc_queue.php');
if ($request !== null) {
    $n = (int) $request->body;
    $server->publish('', (string) $request->property('reply_to'), (string) ($n * $n), [
        'properties' => ['correlation_id' => $request->property('correlation_id')],
    ]);
    $request->ack();
}

// The caller waits for the answer that matches its request.
while (($answer = $answers->next(5.0)) !== null) {
    $answer->ack();
    if ($answer->property('correlation_id') === $correlationId) {
        echo " [.] square(7) = {$answer->body}\n";
        break;
    }
}

$server->close();
$caller->close();
