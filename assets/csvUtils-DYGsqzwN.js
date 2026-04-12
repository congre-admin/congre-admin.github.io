function f(l){const t=l.trim().split(`
`);if(t.length<2)return[];const s=a(t[0]).map(r=>r.trim().replace(/^"|"$/g,""));return t.slice(1).map(r=>{const n=a(r),e={};return s.forEach((o,i)=>{var c;const u=((c=n[i])==null?void 0:c.trim())||"";e[o]=u.replace(/^"|"$/g,"")}),e})}function a(l){const t=[];let s="",r=!1;for(let n=0;n<l.length;n++){const e=l[n],o=l[n+1];r?e==='"'&&o==='"'?(s+='"',n++):e==='"'?r=!1:s+=e:e==='"'?r=!0:e===","?(t.push(s),s=""):s+=e}return t.push(s),t}export{f as p};
//# sourceMappingURL=csvUtils-DYGsqzwN.js.map
