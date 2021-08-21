package otgorm

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

//Attributes that may or may not be added to a span based on Options used
const (
	TableKey = attribute.Key("gorm.table") //The table the GORM query is acting upon
	QueryKey = attribute.Key("gorm.query") //The GORM query itself
)

type callbacks struct {
	//Allow otgorm to create root spans in the absence of a parent span.
	//Default is to not allow root spans.
	allowRoot bool

	//Record the DB query as a KeyValue onto the span where the DB is called
	query bool

	//Record the table that the sql query is acting on
	table bool

	//List of default attributes to include onto the span for DB calls
	defaultAttributes []attribute.KeyValue

	//tracer creates spans. This is required
	tracer trace.Tracer

	//List of default options spans will start with
	spanOptions []trace.SpanStartOption
}

//Gorm scope keys for passing around context and span within the DB scope
var (
	contextScopeKey = "_otContext"
	spanScopeKey    = "_otSpan"
)

// Option allows for managing otgorm configuration using functional options.
type Option interface {
	apply(c *callbacks)
}

// OptionFunc converts a regular function to an Option if it's definition is compatible.
type OptionFunc func(c *callbacks)

func (fn OptionFunc) apply(c *callbacks) {
	fn(c)
}

//WithSpanOptions configures the db callback functions with an additional set of
//trace.SpanStartOption which will be applied to each new span
func WithSpanOptions(opts ...trace.SpanStartOption) OptionFunc {
	return func(c *callbacks) {
		c.spanOptions = opts
	}
}

//WithTracer configures the tracer to use when starting spans. Otherwise
//the global tarcer is used with a default name
func WithTracer(tracer trace.Tracer) OptionFunc {
	return func(c *callbacks) {
		c.tracer = tracer
	}
}

// AllowRoot allows creating root spans in the absence of existing spans.
type AllowRoot bool

func (a AllowRoot) apply(c *callbacks) {
	c.allowRoot = bool(a)
}

// Query allows recording the sql queries in spans.
type Query bool

func (q Query) apply(c *callbacks) {
	c.query = bool(q)
}

//Table allows for recording the table affected by sql queries in spans
type Table bool

func (t Table) apply(c *callbacks) {
	c.table = bool(t)
}

// DefaultAttributes sets attributes to each span.
type DefaultAttributes []attribute.KeyValue

func (d DefaultAttributes) apply(c *callbacks) {
	c.defaultAttributes = []attribute.KeyValue(d)
}

// RegisterCallbacks registers the necessary callbacks in Gorm's hook system for instrumentation with OpenTelemetry Spans.
func RegisterCallbacks(db *gorm.DB, opts ...Option) {
	c := &callbacks{
		defaultAttributes: []attribute.KeyValue{},
	}
	defaultOpts := []Option{
		WithTracer(otel.Tracer("otgorm")),
		WithSpanOptions(trace.WithSpanKind(trace.SpanKindInternal)),
	}

	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(c)
	}

	db.Callback().Create().Before("gorm:create").Register("before_create", c.beforeCreate)
	db.Callback().Create().After("gorm:create").Register("after_create", c.afterCreate)
	db.Callback().Query().Before("gorm:query").Register("before_query", c.beforeQuery)
	db.Callback().Query().After("gorm:query").Register("after_query", c.afterQuery)
	db.Callback().Update().Before("gorm:update").Register("before_update", c.beforeUpdate)
	db.Callback().Update().After("gorm:update").Register("after_update", c.afterUpdate)
	db.Callback().Delete().Before("gorm:delete").Register("before_delete", c.beforeDelete)
	db.Callback().Delete().After("gorm:delete").Register("after_delete", c.afterDelete)
}

func (c *callbacks) before(scope *gorm.DB, operation string) {
	rctx, _ := scope.Get(contextScopeKey)
	ctx, ok := rctx.(context.Context)
	if !ok || ctx == nil {
		ctx = context.Background()
	}

	ctx = c.startTrace(ctx, scope, operation)

	scope.Set(contextScopeKey, ctx)
}

func (c *callbacks) after(scope *gorm.DB) {
	c.endTrace(scope)
}

func (c *callbacks) startTrace(ctx context.Context, scope *gorm.DB, operation string) context.Context {
	// There's no context but we are ok with root spans
	if ctx == nil {
		ctx = context.Background()
	}

	//If there's no parent span and we don't allow root spans, return context
	parentSpan := trace.SpanFromContext(ctx)
	if parentSpan == nil && !c.allowRoot {
		return ctx
	}

	var span trace.Span

	ctx, span = c.tracer.Start(
		ctx,
		fmt.Sprintf("gorm:%s", operation),
		c.spanOptions...,
	)
	scope.Set(spanScopeKey, span)

	return ctx
}

func (c *callbacks) endTrace(scope *gorm.DB) {
	rspan, ok := scope.Get(spanScopeKey)
	if !ok {
		return
	}

	span, ok := rspan.(trace.Span)
	if !ok {
		return
	}

	//Apply span attributes
	attributes := c.defaultAttributes

	if c.table {
		attributes = append(attributes, TableKey.String(scope.Statement.Table))
	}

	if c.query {
		attributes = append(attributes, QueryKey.String(scope.Statement.SQL.String()))
	}
	span.SetAttributes(attributes...)

	//Set StatusCode if there are any issues

	if scope.Error != nil {
		var code codes.Code
		var message string
		err := scope.Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			code = codes.Error
			message = "not found"
		} else {
			code = codes.Unset
			message = "unknown"
		}

		span.SetStatus(code, message)
	}

	span.End()
}

func (c *callbacks) beforeCreate(scope *gorm.DB) { c.before(scope, "create") }
func (c *callbacks) afterCreate(scope *gorm.DB)  { c.after(scope) }
func (c *callbacks) beforeQuery(scope *gorm.DB)  { c.before(scope, "query") }
func (c *callbacks) afterQuery(scope *gorm.DB)   { c.after(scope) }
func (c *callbacks) beforeUpdate(scope *gorm.DB) { c.before(scope, "update") }
func (c *callbacks) afterUpdate(scope *gorm.DB)  { c.after(scope) }
func (c *callbacks) beforeDelete(scope *gorm.DB) { c.before(scope, "delete") }
func (c *callbacks) afterDelete(scope *gorm.DB)  { c.after(scope) }
