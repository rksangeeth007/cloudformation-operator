package main

import (
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/aws/aws-sdk-go/service/cloudformation/cloudformationiface"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/linki/cloudformation-operator/pkg/apis/cloudformation/v1alpha1"
	clientset "github.com/linki/cloudformation-operator/pkg/client/clientset/versioned"
	"context"
	"sync"
	"os/signal"
	"syscall"
)

const (
	ownerTagKey   = "kubernetes.io/controlled-by"
	ownerTagValue = "cloudformation.linki.space/operator"
	clusterIdTagKey   = "kubernetes.io/cluster-id"
)

var (
	master     string
	kubeconfig string
	region     string
	interval   time.Duration
	dryRun     bool
	debug      bool
	version    string
	clusterId string
)

func init() {
	kingpin.Flag("master", "The address of the Kubernetes cluster to target").StringVar(&master)
	kingpin.Flag("kubeconfig", "Path to a kubeconfig file").StringVar(&kubeconfig)
	kingpin.Flag("region", "The AWS region to use").StringVar(&region)
	kingpin.Flag("interval", "Interval between Stack synchronisations").Default("10m").DurationVar(&interval)
	kingpin.Flag("dry-run", "If true, don't actually do anything.").BoolVar(&dryRun)
	kingpin.Flag("debug", "Enable debug logging.").BoolVar(&debug)
	kingpin.Flag("cluster-id", "Identify this cluster and only manage own stacks").Default("acme").StringVar(&clusterId)
}

func main() {
	kingpin.Version(version)
	kingpin.Parse()

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	if dryRun {
		log.Infof("Dry run enabled. I won't change anything.")
	}

	svc := cloudformation.New(session.New(), &aws.Config{
		Region: aws.String(region),
	})

	client, err := newClient()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(interval)
	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := reconcileStacks(svc,client); err != nil {
					log.Errorf("Error reconciling stacks: %s", err)
				}
			}
		}
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	ticker.Stop()
	cancel()
	log.Debug("Waiting for reconcile stacks loop to finish...")
	wg.Wait()
}

func reconcileStacks(svc *cloudformation.CloudFormation, client *clientset.Clientset) error {
	log.Debug("current stacks:")
	currentStacks, err := getCurrentStacks(svc)
	if err != nil {
		return err
	}
	for _, stack := range currentStacks {
		log.Debugf("  %s (%s)\n", aws.StringValue(stack.StackName), aws.StringValue(stack.StackId))
	}

	log.Debug("desired stacks:")
	desiredStacks, err := getDesiredStacks(client)
	if err != nil {
		return err
	}
	for _, stack := range desiredStacks.Items {
		log.Debugf("  %s/%s", stack.Namespace, stack.Name)
	}

	log.Debug("matching stacks:")
	matchingStacks := getMatchingStacks(currentStacks, desiredStacks)
	for _, stack := range matchingStacks.Items {
		log.Debugf("  %s/%s", stack.Namespace, stack.Name)
	}

	log.Debug("superfluous stacks:")
	superfluousStacks := getSuperfluousStacks(currentStacks, desiredStacks)
	for _, stack := range superfluousStacks {
		log.Debugf("  %s (%s)", aws.StringValue(stack.StackName), aws.StringValue(stack.StackId))
	}

	log.Debug("missing stacks:")
	missingStacks := getMissingStacks(currentStacks, desiredStacks)
	for _, stack := range missingStacks.Items {
		log.Debugf("  %s/%s", stack.Namespace, stack.Name)
	}

	for _, stack := range matchingStacks.Items {
		if err := updateStack(svc, client, stack); err != nil {
			log.Errorf("Unable to update stack %s : %s - Skipping!", stack.Name, err)
		}
	}

	for _, stack := range superfluousStacks {
		if err := deleteStack(svc, stack); err != nil {
			log.Errorf("Unable to delete stack %s : %s - Skipping!", aws.StringValue(stack.StackName), err)
		}
	}

	for _, stack := range missingStacks.Items {
		if err := createStack(svc, client, stack); err != nil {
			log.Errorf("Unable to create stack %s : %s - Skipping!", stack.Name, err)
		}
	}

	return nil
}

func getCurrentStacks(svc cloudformationiface.CloudFormationAPI) ([]*cloudformation.Stack, error) {
	stacks, err := svc.DescribeStacks(&cloudformation.DescribeStacksInput{})
	if err != nil {
		return nil, err
	}

	ownedStacks := []*cloudformation.Stack{}

	for _, stack := range stacks.Stacks {
		isOperatorManaged := false
		isSameClusterId := false
		for _, tag := range stack.Tags {
			if aws.StringValue(tag.Key) == ownerTagKey && aws.StringValue(tag.Value) == ownerTagValue {
				isOperatorManaged = true
			} else if aws.StringValue(tag.Key) == clusterIdTagKey && aws.StringValue(tag.Value) == clusterId {
				isSameClusterId = true
			}
		}
		if isOperatorManaged && isSameClusterId {
			ownedStacks = append(ownedStacks, stack)
		}
	}

	return ownedStacks, nil
}

func getDesiredStacks(client clientset.Interface) (*v1alpha1.StackList, error) {
	return client.CloudformationV1alpha1().Stacks(v1.NamespaceAll).List(metav1.ListOptions{})
}

func getMatchingStacks(current []*cloudformation.Stack, desired *v1alpha1.StackList) v1alpha1.StackList {
	stackList := v1alpha1.StackList{Items: []v1alpha1.Stack{}}

	for _, ds := range desired.Items {
		for _, cs := range current {
			if aws.StringValue(cs.StackName) == ds.Name {
				stackList.Items = append(stackList.Items, ds)
			}
		}
	}

	return stackList
}

func getSuperfluousStacks(current []*cloudformation.Stack, desired *v1alpha1.StackList) []*cloudformation.Stack {
	stacks := []*cloudformation.Stack{}

	for _, cs := range current {
		found := false

		for _, ds := range desired.Items {
			if aws.StringValue(cs.StackName) == ds.Name {
				found = true
			}
		}

		if !found {
			stacks = append(stacks, cs)
		}
	}

	return stacks
}

func getMissingStacks(current []*cloudformation.Stack, desired *v1alpha1.StackList) v1alpha1.StackList {
	stackList := v1alpha1.StackList{
		Items: []v1alpha1.Stack{},
	}

	for _, ds := range desired.Items {
		found := false

		for _, cs := range current {
			if aws.StringValue(cs.StackName) == ds.Name {
				found = true
			}
		}

		if !found {
			stackList.Items = append(stackList.Items, ds)
		}
	}

	return stackList
}

func createStack(svc cloudformationiface.CloudFormationAPI, client clientset.Interface, stack v1alpha1.Stack) error {
	log.Infof("creating stack: %s", stack.Name)

	if dryRun {
		log.Info("skipping...")
		return nil
	}

	params := []*cloudformation.Parameter{}
	for k, v := range stack.Spec.Parameters {
		params = append(params, &cloudformation.Parameter{
			ParameterKey:   aws.String(k),
			ParameterValue: aws.String(v),
		})
	}

	input := &cloudformation.CreateStackInput{
		StackName:    aws.String(stack.Name),
		TemplateBody: aws.String(stack.Spec.Template),
		Parameters:   params,
		Tags: []*cloudformation.Tag{
			{
				Key:   aws.String(ownerTagKey),
				Value: aws.String(ownerTagValue),
			},
		},
	}
	if _, err := svc.CreateStack(input); err != nil {
		return err
	}

	log.Infof("Waiting for create stack %s to complete", stack.Name)
	for {
		foundStack, err := getStack(svc, stack.Name)
		if err != nil {
			return err
		}

		log.Debugf("%s stack status: %s", stack.Name, aws.StringValue(foundStack.StackStatus))

		if aws.StringValue(foundStack.StackStatus) != cloudformation.StackStatusCreateInProgress {
			log.Infof("Create stack %s completed with status: %s", stack.Name, aws.StringValue(foundStack.StackStatus))
			break
		}

		time.Sleep(time.Second)
	}

	foundStack, err := getStack(svc, stack.Name)
	if err != nil {
		return err
	}

	stackCopy := stack.DeepCopy()
	stackCopy.Status.StackID = aws.StringValue(foundStack.StackId)

	stackCopy.Status.Outputs = map[string]string{}
	for _, output := range foundStack.Outputs {
		stackCopy.Status.Outputs[aws.StringValue(output.OutputKey)] = aws.StringValue(output.OutputValue)
	}

	if _, err := client.CloudformationV1alpha1().Stacks(stack.Namespace).Update(stackCopy); err != nil {
		return err
	}

	return nil
}

func updateStack(svc cloudformationiface.CloudFormationAPI, client clientset.Interface, stack v1alpha1.Stack) error {
	log.Debugf("updating stack: %s", stack.Name)

	if dryRun {
		log.Info("skipping...")
		return nil
	}

	params := []*cloudformation.Parameter{}
	for k, v := range stack.Spec.Parameters {
		params = append(params, &cloudformation.Parameter{
			ParameterKey:   aws.String(k),
			ParameterValue: aws.String(v),
		})
	}

	input := &cloudformation.UpdateStackInput{
		StackName:    aws.String(stack.Name),
		TemplateBody: aws.String(stack.Spec.Template),
		Parameters:   params,
	}

	if _, err := svc.UpdateStack(input); err != nil {
		if strings.Contains(err.Error(), "No updates are to be performed.") {
			log.Debug("Stack update not needed.")
			return nil
		}
		return err
	}

	log.Infof("Waiting for update stack %s to complete", stack.Name)
	for {
		foundStack, err := getStack(svc, stack.Name)
		if err != nil {
			return err
		}

		log.Debugf("%s stack status: %s", stack.Name, aws.StringValue(foundStack.StackStatus))

		if aws.StringValue(foundStack.StackStatus) != cloudformation.StackStatusUpdateInProgress {
			log.Infof("Update stack %s completed with status: %s", stack.Name, aws.StringValue(foundStack.StackStatus))
			break
		}

		time.Sleep(time.Second)
	}

	foundStack, err := getStack(svc, stack.Name)
	if err != nil {
		return err
	}

	stackCopy := stack.DeepCopy()
	stackCopy.Status.StackID = aws.StringValue(foundStack.StackId)

	stackCopy.Status.Outputs = map[string]string{}
	for _, output := range foundStack.Outputs {
		stackCopy.Status.Outputs[aws.StringValue(output.OutputKey)] = aws.StringValue(output.OutputValue)
	}

	if _, err := client.CloudformationV1alpha1().Stacks(stack.Namespace).Update(stackCopy); err != nil {
		return err
	}

	return nil
}

func deleteStack(svc cloudformationiface.CloudFormationAPI, stack *cloudformation.Stack) error {
	log.Infof("deleting stack: %s", aws.StringValue(stack.StackName))

	if dryRun {
		log.Info("skipping...")
		return nil
	}

	input := &cloudformation.DeleteStackInput{
		StackName: stack.StackName,
	}

	if _, err := svc.DeleteStack(input); err != nil {
		return err
	}

	log.Infof("Waiting for delete stack %s to complete", aws.StringValue(stack.StackName))
	for {
		foundStack, err := getStack(svc, aws.StringValue(stack.StackName))
		if err != nil {
			return err
		}

		if foundStack == nil {
			break
		}

		log.Debug("%s stack status: %s", aws.StringValue(stack.StackName), aws.StringValue(foundStack.StackStatus))

		if aws.StringValue(foundStack.StackStatus) != cloudformation.StackStatusDeleteInProgress {
			log.Infof("Delete stack %s completed with status: %s", aws.StringValue(stack.StackName), aws.StringValue(foundStack.StackStatus))
			break
		}

		time.Sleep(time.Second)
	}

	return nil
}

func getStack(svc cloudformationiface.CloudFormationAPI, name string) (*cloudformation.Stack, error) {
	resp, err := svc.DescribeStacks(&cloudformation.DescribeStacksInput{
		StackName: aws.String(name),
	})
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	if len(resp.Stacks) == 0 {
		return nil, nil
	}

	return resp.Stacks[0], nil
}

func newClient() (*clientset.Clientset, error) {
	if kubeconfig == "" {
		if _, err := os.Stat(clientcmd.RecommendedHomeFile); err == nil {
			kubeconfig = clientcmd.RecommendedHomeFile
		}
	}

	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		return nil, err
	}

	log.Infof("Targeting cluster at %s", config.Host)

	client, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return client, nil
}
