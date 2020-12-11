#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use FindBin qw($Bin);
use lib "$Bin/lib";
use Carp qw(croak);
use MemcachedTest;

# TODO: to module?
# or "gettimedrun" etc
use Cwd;
my $builddir = getcwd;

# TODO: the lua file has hardcoded ports. any way to make this dynamic?
# TODO: once basic tests are done, actually split out the instances rather
# than the shared backend; validate keys go where they should be going.

# FIXME: this listend on unix socket still. either need a manual runner or a
# fix upstream.
my $srv = run_server('-p 11212', 11212);
my $sock = $srv->sock;

my $p_srv = new_memcached('-o proxy_config=./t/startfile.lua');
my $p_sock = $p_srv->sock;

# set through proxy.
{
    print $p_sock "set /foo/z 0 0 5\r\nhello\r\n";
    is(scalar <$p_sock>, "STORED\r\n", "stored test value through proxy");
}

# Load some keys to backend server.
my $bdata = 'x' x 256000;
{
    for (1..20) {
        print $sock "set /foo/a$_ 0 0 2\r\nhi\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored test value");
        print $sock "set /bar/b$_ 0 0 2\r\nhi\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored test value");
    }

    # load a couple larger values
    for (1..4) {
        print $sock "set /foo/big$_ 0 0 256000\r\n$bdata\r\n";
        is(scalar <$sock>, "STORED\r\n", "stored big value");
    }
}

# fetch through proxy.
{
    for (1..20) {
        mem_get_is($p_sock, "/foo/a$_", "hi");
    }
    mem_get_is($p_sock, "/foo/big1", $bdata);
}

sub run_server {
    my ($args, $port) = @_;

    my $exe = get_memcached_exe();

    my $childpid = fork();

    my $root = '';
    $root = "-u root" if ($< == 0);

    # test build requires more privileges
    $args .= " -o relaxed_privileges";

    my $cmd = "$builddir/timedrun 120 $exe $root $args";

    unless($childpid) {
        exec $cmd;
        exit; # NOTREACHED
    }

    for (1..20) {
        my $conn = IO::Socket::INET->new(PeerAddr => "127.0.0.1:$port");
        if ($conn) {
            return Memcached::Handle->new(pid  => $childpid,
                conn => $conn,
                host => "127.0.0.1",
                port => $port);
        }
        select undef, undef, undef, 0.10;
    }
    croak "Failed to start server.";
}

done_testing();
