package main

import (
    "context"
    "errors"
    "flag"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "path/filepath"
    "strings"

    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/clientcmd"
    "k8s.io/client-go/util/homedir"
    "k8s.io/apimachinery/pkg/watch"

    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func main() {
    var namespace, service, nodesFile string
    var peerPort, apiPort int
    var verbose bool

    flag.StringVar(&namespace, "namespace", "typesense", "The namespace that Typesense is installed within")
    flag.StringVar(&service, "service", "ts", "The name of the Typesense service to use the endpoints of")
    flag.StringVar(&nodesFile, "nodes-file", "/usr/share/typesense/nodes", "The location of the file to write node information to")
    flag.IntVar(&peerPort, "peer-port", 8107, "Port on which Typesense peering service listens")
    flag.IntVar(&apiPort, "api-port", 8108, "Port on which Typesense API service listens")
    flag.BoolVar(&verbose, "verbose", false, "Enable verbose logging")
    flag.Parse()

    configPath := filepath.Join(homedir.HomeDir(), ".kube", "config")

    var config *rest.Config
    var err error

    if _, err = os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
        // No config file found, fall back to in-cluster config.
        config, err = rest.InClusterConfig()
        if err != nil {
            log.Fatalf("failed to build local config: %s\n", err)
            return
        }
    } else {
        config, err = clientcmd.BuildConfigFromFlags("", configPath)
        if err != nil {
            log.Fatalf("failed to build in-cluster config: %s\n", err)
            return
        }
    }

    clients, err := kubernetes.NewForConfig(config)
    if err != nil {
        log.Fatalf("failed to create kubernetes client: %s\n", err)
        return
    }

    endpoints, err := clients.CoreV1().Endpoints(namespace).List(context.Background(), metav1.ListOptions{})
    if err != nil {
        log.Fatalf("failed to list endpoints: %s\n", err)
        return
    }

    var matchedEndpoints []string
    for _, e := range endpoints.Items {
        if e.Name == service {
            matchedEndpoints = append(matchedEndpoints, e.String())
        }
    }

    if len(matchedEndpoints) == 0 {
        log.Printf("Initial run, no endpoints found for service: %s\n", service)
    }

    if verbose {
        log.Printf("Initial run, found %d endpoints:\n%s\n", len(matchedEndpoints), strings.Join(matchedEndpoints, "\n"))
    }

    nodes := getNodes(clients, namespace, service, peerPort, apiPort, verbose)
    if nodes == "" {
        log.Printf("No nodes found on initial run")
    }

    err = os.WriteFile(nodesFile, []byte(nodes), 0666)
    if err != nil {
        log.Fatalf("failed to write nodes file: %s\n", err)
        return
    }

    watcher, err := clients.CoreV1().Endpoints(namespace).Watch(context.Background(), metav1.ListOptions{})
    if err != nil {
        log.Fatalf("failed to create endpoints watcher: %s\n", err)
        return
    }
    
    doneCh := make(chan struct{})
    go func() {
        // Wait for SIGTERM, SIGINT or SIGKILL
        signals := make(chan os.Signal, 1)
        signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
    
        // Block until we receive a signal or the done channel is closed.
        select {
        case sig := <-signals:
            log.Printf("Received signal %v, stopping watcher.", sig)
            close(doneCh)
        case <-doneCh:
            log.Print("Watcher stopped.")
        }
    }()    

    for {
        select {
        case <-doneCh:
            return
        case res, ok := <-watcher.ResultChan():
            if !ok {
                watcher, err = clients.CoreV1().Endpoints(namespace).Watch(context.Background(), metav1.ListOptions{})
                if err != nil {
                    log.Fatalf("failed to create endpoints watcher: %s\n", err)
                    close(doneCh)
                    return
                }
                log.Print("watcher channel closed, but successfully reconnected. Continuing...")
                continue
            }
    
            if res.Type == watch.Modified || res.Type == watch.Deleted || res.Type == watch.Added {
                nodes := getNodes(clients, namespace, service, peerPort, apiPort, verbose)
                if nodes == "" {
                    log.Print("nodes string is empty, skipping write to file.")
                    continue
                }
    
                err := os.WriteFile(nodesFile, []byte(nodes), 0666)
                if err != nil {
                    log.Printf("failed to write nodes file: %s\n", err)
                    continue
                }
            }
        }
    }
}

func getNodes(clients *kubernetes.Clientset, namespace, service string, peerPort int, apiPort int, verbose bool) string {
    var nodes []string

    endpoints, err := clients.CoreV1().Endpoints(namespace).List(context.Background(), metav1.ListOptions{})
    if err != nil {
        log.Printf("failed to list endpoints: %s\n", err)
        return ""
    }

    if len(endpoints.Items) == 0 {
        log.Printf("No endpoints found for service: %s\n", service)
        return ""
    }

    for _, e := range endpoints.Items {
        if e.Name != service {
            continue
        }

        for _, s := range e.Subsets {
            for _, a := range s.Addresses {
                if verbose {
                    log.Printf("Handling address %s", a.IP)
                }
                for _, p := range s.Ports {
                    if verbose {
                        log.Printf("Handling port %d for address %s", p.Port, a.IP)
                    }                    
                    // Typesense exporter sidecar for Prometheus runs on port 9000
                    if int(p.Port) == peerPort {
                        nodes = append(nodes, fmt.Sprintf("%s:%d:%d", a.IP, peerPort, apiPort))
                    } else {
                        log.Printf("Found port %d did not match apiPort %d", p.Port, peerPort)
                    }
                }
            }
        }
    }

    if len(nodes) == 0 {
        if verbose {
            log.Printf("No nodes found for service: %s\n", service)
        }        
        return ""
    }

    typesenseNodes := strings.Join(nodes, ",")

    if verbose && len(nodes) != 0 {
        log.Printf("Watcher update, new %d node configuration: %s\n", len(nodes), typesenseNodes)
    }

    return typesenseNodes
}