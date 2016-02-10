'use strict';

var mongoose = require('mongoose'),
	natural = require('natural'),
    stringify = require('json-stable-stringify'),
	_ = require('underscore');


var searchResultScheme = mongoose.Schema({
	__collection: String,
	query: [String],
    conditions: String,
	results: [mongoose.Schema.Types.Mixed],
	createdAt: {type: Date, default: Date.now, expires: 3600 }
});

searchResultScheme.index({collection: 1, searchString: 1});

var SearchResult = mongoose.model('SearchResult', searchResultScheme);

module.exports = function(schema, options) {
	var stemmer = natural[options.stemmer || 'PorterStemmer'],
		distance = natural[options.distance || 'JaroWinklerDistance'],
		fields = options.fields,
		keywordsPath = options.keywordsPath || '_keywords',
		relevancePath = options.relevancePath || '_relevance';

	// init keywords field
	var schemaMixin = {};
	schemaMixin[keywordsPath] = [String];
	schemaMixin[relevancePath] = Number;
	schema.add(schemaMixin);
	schema.path(keywordsPath).index(true);

	// search method
	schema.statics.search = function(query, fields, options, callback) {
		if (arguments.length === 2) {
			callback = fields;
			options = {};
		} else {
			if (arguments.length === 3) {
				callback = options;
				options = {};
			} else {
				options = options || {};
			}
		}

		var self = this;
		var tokens = _(stemmer.tokenizeAndStem(query)).unique(),
            findOptions = _(options).pick('sort');

        getSearchResult(tokens, findOptions, function(err, ids) {
            if (err) return callback(err);

            var totalCount = ids.length;

            // slice results and find full objects by ids
            if (options.limit || options.skip) {
                options.skip = options.skip || 0;
                options.limit = options.limit || (ids.length - options.skip);
                ids = ids.slice(options.skip || 0, options.skip + options.limit);
            }

            function processDocs(err, docs) {
                if (err) return callback(err);

                var data = { totalCount : totalCount };

                if (findOptions.sort) {
                    data.results = docs;
                } else {
                    data.results = _(docs).sortBy(function(doc){
                        return ids.indexOf(doc._id);
                    });
                }

                callback(null, data);
            }

            if (options.aggregate) {

                var aggregate = [
                    { $match: { _id: { $in: ids } } },
                    { $limit: ids.length }
                ].concat(options.aggregate);

                mongoose.Model.aggregate.call(self, aggregate, processDocs);
            } else {
                var findConditions = _({
                    _id: {$in: ids}
                }).extend(options.conditions);

                var cursor = mongoose.Model.find
                    .call(self, findConditions, fields, findOptions);

                // populate
                if (options.populate) {
                    options.populate.forEach(function(object) {
                        cursor.populate(object.path, object.fields);
                    });
                }
                cursor.exec(processDocs);
            }
        });

        function getSearchResult(tokens, findOptions, callback) {

            var query = {
                __collection: self.modelName,
                query: tokens
            };

            if (options.conditions) {
                query.conditions = stringify(options.conditions);
            }

            // Check cache
            SearchResult.findOne(query,
                function(err, doc) {
                    if (err) return callback(err);
                    if (doc) return callback(null, doc.results);

                    // find and save result into cache if no cache entry was found
                    findResult(tokens, findOptions, callback);
                });
        }

        function findResult(tokens, findOptions, callback) {
            var outFields = {_id: 1};
            var conditions = options.conditions || {};

            conditions[keywordsPath] = {$in: tokens};
            outFields[keywordsPath] = 1;

            mongoose.Model.find.call(self, conditions, outFields, findOptions,
            function(err, docs) {
                if (err) return callback(err);
                if (!findOptions.sort) {
                    docs = _(docs).sortBy(function(doc){
                        var relevance = processRelevance(tokens, doc.get(keywordsPath));
                        doc.set(relevancePath, relevance);
                        return -relevance;
                    });
                }

                var ids = _.pluck(docs, '_id');
                var result = new SearchResult({
                    __collection: self.modelName,
                    query: tokens,
                    results: ids
                });

                if (options.conditions) {
                    result.conditions = stringify(options.conditions);
                }

                result.save(function(err) {
                    if (err) return callback(err);
                    callback(null, ids);
                });
            });

        }

		function processRelevance(queryTokens, resultTokens) {
			var relevance = 0;

			queryTokens.forEach(function(token) {
				relevance += tokenRelevance(token, resultTokens);
			});
			return relevance;
		}

		function tokenRelevance(token, resultTokens) {
			var relevanceThreshold = 0.5,
				result = 0;

			resultTokens.forEach(function(rToken) {
				var relevance = distance(token, rToken);
				if (relevance > relevanceThreshold) {
					result += relevance;
				}
			});

			return result;
		}
	};

	// set keywords for all docs in db
	schema.statics.setKeywords = function(callback) {
		callback = _(callback).isFunction() ? callback : function() {};

		mongoose.Model.find.call(this, {}, function(err, docs) {
			if (err) return callback(err);

			if (docs.length) {
				var done = _.after(docs.length, function() {
					callback();
				});
				docs.forEach(function(doc) {
					doc.updateKeywords();

					doc.save(function(err) {
						if (err) console.log('[mongoose search plugin err] ', err, err.stack);
						done();
					});
				});
			} else {
				callback();
			}
		});
	};

	schema.methods.updateKeywords = function() {
		this.set(keywordsPath, this.processKeywords());
	};

	schema.methods.processKeywords = function() {
		var self = this;
		return _(stemmer.tokenizeAndStem(fields.map(function(field) {
			var val = self.get(field);

			if (_(val).isString()) {
				return val;
			}
			if (_(val).isArray()) {
				return val.join(' ');
			}

			return '';
		}).join(' '))).unique();
	};

	schema.pre('save', function(next) {
		var self = this;

	    var isChanged = this.isNew || fields.some(function (field) {
	      return self.isModified(field);
	    });

	    if (isChanged) this.updateKeywords();
	    next();
	});
};
