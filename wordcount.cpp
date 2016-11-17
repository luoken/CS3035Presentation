#include <algorithm>
#include <limits>
#include <string>

#include  "stdint.h" 
#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

using namespace std;

class WordCountMapper : public HadoopPipes::Mapper {
public:

  void map( HadoopPipes::MapContext&context) {
    string line = context.getInputValue();
    vector<string> words = HadoopUtils::splitString(line, " ");
    for (unsigned int i=0; i < words.size(); i++){
      context.emit(words[i], HadoopUtils::toString(1));
    }
  }
};
 
class WordCountReducer : public HadoopPipes::Reducer {
public:

  void reduce( HadoopPipes::ReduceContext&context) {
    int count = 0;
    while (context.nextValue()) {
      count += HadoopUtils::toInt(context.getInputValue());
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(count));
  }
};
 
int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<WordCountMapper, WordCountReducer>());
}
