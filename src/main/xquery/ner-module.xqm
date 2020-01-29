xquery version "3.1";

(:~
 : A very simple example XQuery Library Module implemented
 : in XQuery.
 :)
module namespace nerzh = "http://exist-db.org/xquery/stanford-nlp/ner/chinese";

import module namespace nlp-zh="http://exist-db.org/xquery/stanford-nlp/chinese";
import module namespace functx = "http://www.functx.com";
import module namespace console = "http://exist-db.org/xquery/console";

declare namespace rest = "http://exquery.org/ns/restxq";

declare
  %rest:path("/StanfordNLP/NER/chinese")
  %rest:PUT("{$request-body}")
function nerzh:classify-document($request-body as document-node(element())) {
    let $annotators := fn:json-doc("/db/apps/stanford-nlp-chinese/data/StanfordCoreNLP-chinese.json")
    return nerzh:dispatch($request-body/node(), $annotators)
};

declare
function nerzh:classify-node($node as node()) {
    let $annotators := fn:json-doc("/db/apps/stanford-nlp-chinese/data/StanfordCoreNLP-chinese.json")
    return nerzh:dispatch($node, $annotators)
};

declare
    %rest:path("/StanfordNLP/NER/chinese")
    %rest:form-param("text", "{$text}")
function nerzh:classify-text($text as xs:string) {
    let $annotators := fn:json-doc("/db/apps/stanford-nlp-chinese/data/StanfordCoreNLP-chinese.json")
    return nerzh:classify($node/text(), $annotators)
};

declare function nerzh:sibling($token as node(), $tokens as node()*) as node() {
    if (count($tokens) = 0) then $token else
    let $next-token := $tokens[1]
    let $next-seq := fn:subsequence($tokens, 2)
    let $token-index := xs:integer($token/@id)
    let $next-token-index := xs:integer($next-token/@id)
    return
    if ($next-token-index = ($token-index - 1))
    then if ($next-token/NER/text() eq $token/NER/text())
    then
        let $return-token := nerzh:sibling($next-token, $next-seq)
        return $return-token
    else $token
    else $token
};

declare function nerzh:enrich($text as xs:string, $tokens as node()*) {
    if (fn:count($tokens) eq 0)
    then 
        $text
    else    
        let $last-token := $tokens[1]
        let $sibling-token := nerzh:sibling($last-token, fn:subsequence($tokens, 2))
        let $sibling-position := fn:index-of($tokens, $sibling-token)
        let $start := $sibling-token/CharacterOffsetBegin/number() + 1
        let $end := $last-token/CharacterOffsetEnd/number() + 1
        let $length := $end - $start
        let $before := fn:substring($text, 1, $start - 1)
        let $after := fn:substring($text, $end)
        let $ner-text := fn:substring($text, $start, $length)
        let $next := fn:subsequence($tokens, fn:index-of($tokens, $sibling-token) + 1)
        return (
            nerzh:enrich($before, $next), 
            element { $last-token/NER/text() } { $ner-text }, 
            if (fn:string-length($after) gt 0) then $after else ())
    
};

declare function nerzh:dispatch($node as node()?, $annotators as map(*)) {
    if ($node)
    then
        if (functx:has-simple-content($node))
        then element { $node/name() } { $node/@*, nerzh:classify($node/text(), $annotators) }
        else nerzh:pass-through($node, $annotators)
        else ()
};

declare function nerzh:pass-through($node as node()?, $annotators as map(*)) {
    if ($node)
    then element { $node/name() } { 
        $node/@*,  
        for $cnode in $node/* 
        return nerzh:dispatch($cnode, $annotators)
    }
    else ()
};

declare function nerzh:classify($text as xs:string, $annotators as map(*)) {
let $tokens := for $token in nlp-zh:parse($text, $annotators)//token[fn:not(NER = "O")]
                let $token-start := $token/CharacterOffsetBegin/number()
                order by $token-start descending
            return $token
return nerzh:enrich($text, $tokens)    
};
