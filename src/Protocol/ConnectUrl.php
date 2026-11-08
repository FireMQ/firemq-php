<?php

declare(strict_types=1);

namespace FireMQ\Protocol;

/**
 * A parsed `fire://`, `fires://`, `ws://` or `wss://` connection URL.
 *
 * Defaults: user and password `guest`, vhost `/`, ports 5673 (`fire`), 5674 (`fires`),
 * 15672 (`ws`) and 15671 (`wss`). For `fire` and `fires` the path is the vhost
 * (`fire://host/my-vhost`, percent-decoded). For `ws` and `wss` the path is the endpoint
 * (`/fire` when empty) and the vhost is the `vhost` query parameter.
 */
final class ConnectUrl
{
    private const DEFAULT_PORTS = [
        'fire' => 5673,
        'fires' => 5674,
        'ws' => 15672,
        'wss' => 15671,
    ];

    private function __construct(
        public readonly string $scheme,
        public readonly string $host,
        public readonly int $port,
        public readonly string $vhost,
        public readonly string $user,
        public readonly string $password,
        public readonly string $path,
    ) {
    }

    /**
     * @throws \InvalidArgumentException when the URL is invalid or uses another scheme
     */
    public static function parse(string $url): self
    {
        $parts = parse_url($url);
        if ($parts === false || !isset($parts['scheme'])) {
            throw new \InvalidArgumentException(sprintf('invalid URL: %s', self::redact($url)));
        }
        $scheme = strtolower($parts['scheme']);
        if (!isset(self::DEFAULT_PORTS[$scheme])) {
            throw new \InvalidArgumentException(sprintf('unsupported scheme "%s"; use fire, fires, ws or wss', $scheme));
        }
        $host = $parts['host'] ?? '';
        if (str_starts_with($host, '[') && str_ends_with($host, ']')) {
            $host = substr($host, 1, -1);
        }
        if ($host === '') {
            $host = 'localhost';
        }
        $user = isset($parts['user']) && $parts['user'] !== '' ? rawurldecode($parts['user']) : 'guest';
        $password = isset($parts['pass']) ? rawurldecode($parts['pass']) : 'guest';
        $query = [];
        if (isset($parts['query'])) {
            parse_str($parts['query'], $query);
        }
        $queryVhost = isset($query['vhost']) && is_string($query['vhost']) ? $query['vhost'] : null;
        $rawPath = $parts['path'] ?? '';
        if ($scheme === 'fire' || $scheme === 'fires') {
            $trimmed = ltrim($rawPath, '/');
            $vhost = $trimmed === '' ? ($queryVhost ?? '/') : rawurldecode($trimmed);
            $path = '';
        } else {
            $vhost = $queryVhost ?? '/';
            $path = $rawPath === '' || $rawPath === '/' ? '/fire' : $rawPath;
        }

        return new self(
            $scheme,
            $host,
            $parts['port'] ?? self::DEFAULT_PORTS[$scheme],
            $vhost,
            $user,
            $password,
            $path,
        );
    }

    /**
     * True for `fires://` and `wss://`.
     */
    public function isTls(): bool
    {
        return $this->scheme === 'fires' || $this->scheme === 'wss';
    }

    /**
     * `host:port`, with IPv6 hosts bracketed.
     */
    public function authority(): string
    {
        return str_contains($this->host, ':')
            ? sprintf('[%s]:%d', $this->host, $this->port)
            : sprintf('%s:%d', $this->host, $this->port);
    }

    /**
     * The URL without the password, for messages and logs.
     */
    public function __toString(): string
    {
        $suffix = match ($this->scheme) {
            'fire', 'fires' => '/' . ($this->vhost === '/' ? '' : rawurlencode($this->vhost)),
            default => $this->path . ($this->vhost === '/' ? '' : '?vhost=' . rawurlencode($this->vhost)),
        };

        return sprintf('%s://%s@%s%s', $this->scheme, rawurlencode($this->user), $this->authority(), $suffix);
    }

    private static function redact(string $url): string
    {
        return (string) preg_replace('#//([^:/@]*):[^@/]*@#', '//$1:***@', $url);
    }
}
