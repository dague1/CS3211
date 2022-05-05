#include "engine.hpp"
#include <iostream>
#include <thread>
#include "io.h"
#include <chrono>
#include <mutex>
#include <optional>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <string>
#include <vector>
// Assignment done by David Ljunggren and XD

// A data structure for each instrument order created. 
// Buy orders and sell orders are stored in separate vectors.
// Each container has its own mutex. 

struct InstrumentOrders
{
  std::mutex m;
  std::vector<input> buyOrders;
  std::vector<input> sellOrders;
};

std::unordered_map<std::string, InstrumentOrders> orderMap;
std::unordered_map<std::uint32_t, input> inputIdMap;
std::mutex m2;
void manageSellOrders(input &input, int64_t input_time)
{

  auto &instrumentOrder = orderMap[std::string(input.instrument)];
  bool matched = false;
  u_int32_t remainingCount = input.count;
  {
    std::scoped_lock lock(instrumentOrder.m);
    
    for (auto it = instrumentOrder.buyOrders.begin(); it != instrumentOrder.buyOrders.end() && remainingCount > 0;)
    {

      if (it->price >= input.price)
      {

        if (it->count > remainingCount)
        {
          it->count -= remainingCount;
          it->executionID++;
          int oldRemainingCount = remainingCount;
          remainingCount = 0;
          matched = true;
          Output::OrderExecuted(it->order_id, input.order_id, it->executionID, it->price, oldRemainingCount, input_time, CurrentTimestamp());

          break;
        }
        else
        {
          remainingCount -= it->count;
          it->executionID++;
          {
          std::scoped_lock lock(m2); 
          inputIdMap.erase(it->order_id); // Remove the ID from the id-map for the cancel function to work properly.
          }
          matched = true;
          Output::OrderExecuted(it->order_id, input.order_id, it->executionID, it->price, it->count, input_time, CurrentTimestamp());

          it = instrumentOrder.buyOrders.erase(it);
          continue;
        }
      }
            else {
        Output::OrderAdded(input.order_id, input.instrument, input.price,
                         input.count, true,
                         input_time, CurrentTimestamp());
      }
      ++it;
    }
    if (remainingCount > 0)
    {

      input.count = remainingCount;
      instrumentOrder.sellOrders.push_back(input);
      {
      std::scoped_lock lock(m2); 
      inputIdMap.insert({input.order_id, input});
      }
      if(matched == true) {
              Output::OrderAdded(input.order_id, input.instrument, input.price,
                         input.count, input.type,
                         input_time, CurrentTimestamp());
      }
            
    }     
  }
  if(matched==false) {
   
        Output::OrderAdded(input.order_id, input.instrument, input.price,
                         input.count, true,
                         input_time, CurrentTimestamp());
      
  }
}

void manageBuyOrders(input &input, int64_t input_time)
{

  auto &instrumentOrder = orderMap[std::string(input.instrument)];
  bool matched = false;
  u_int32_t remainingCount = input.count;
  {
    std::scoped_lock lock(instrumentOrder.m);
    for (auto it = instrumentOrder.sellOrders.begin(); it != instrumentOrder.sellOrders.end() && remainingCount > 0;)
    {

      if (it->price <= input.price)
      {

        if (it->count > remainingCount)
        {
          it->count -= remainingCount;
          it->executionID++;
          int oldRemainingCount = remainingCount;
          remainingCount = 0;
          matched = true;
          Output::OrderExecuted(it->order_id, input.order_id, it->executionID, it->price, oldRemainingCount, input_time, CurrentTimestamp());
          break;
        }
        else
        {
          remainingCount -= it->count;
          it->executionID++;
          {
            std::scoped_lock lock(m2); 
            inputIdMap.erase(it->order_id);
          }
          
          matched = true;
          Output::OrderExecuted(it->order_id, input.order_id, it->executionID, it->price, (it->count), input_time, CurrentTimestamp());
          it = instrumentOrder.sellOrders.erase(it);
          continue;
        }
      }
      
      ++it;
    }
    if (remainingCount > 0)
    {

      input.count = remainingCount;
      instrumentOrder.buyOrders.push_back(input);
      {
            std::scoped_lock lock(m2); 
            inputIdMap.insert({input.order_id, input});
      }
      
      if(matched==true) {
              Output::OrderAdded(input.order_id, input.instrument, input.price,
              input.count, false,
              input_time, CurrentTimestamp());
      }
    }

  }
  if(matched==false) {
        Output::OrderAdded(input.order_id, input.instrument, input.price,
                         input.count, false,
                         input_time, CurrentTimestamp());
      
  }
}

void cancelOrder(input &input, int64_t input_time)
{
  bool found = false;
  auto &instrumentOrder = orderMap[std::string(input.instrument)];
  {
    std::scoped_lock lock(instrumentOrder.m);
    if (input.type == input_buy)
    {
      for (auto it = instrumentOrder.buyOrders.begin(); it != instrumentOrder.buyOrders.end(); ++it)
      {
        if (input.order_id == it->order_id)
        {
          instrumentOrder.buyOrders.erase(it);
          found = true;
          break;
        }
      }
    }
    else
    {
      for (auto it = instrumentOrder.sellOrders.begin(); it != instrumentOrder.sellOrders.end(); ++it)
      {
        if (input.order_id == it->order_id)
        {
          instrumentOrder.sellOrders.erase(it);
          found = true;
          break;
        }
      }
    }
  }
  Output::OrderDeleted(input.order_id, found, input_time, CurrentTimestamp());
}

void Engine::Accept(ClientConnection connection)
{
  std::thread thread{&Engine::ConnectionThread, this,
                     std::move(connection)};
  thread.detach();
}

void Engine::ConnectionThread(ClientConnection connection)
{
  while (true)
  {
    input input;
    input.executionID = 0;
    switch (connection.ReadInput(input))
    {
    case ReadResult::Error:
      std::cerr << "Error reading input" << std::endl;
    case ReadResult::EndOfFile:
      return;
    case ReadResult::Success:
      break;
    }
    int64_t input_time = CurrentTimestamp();
    // Functions for printing output actions in the prescribed format are
    // provided in the Output class:
    switch (input.type)
    {
    case input_cancel:
    {

      auto it = inputIdMap.find(input.order_id);
      if (it != inputIdMap.end())
      {
       // std::cout << "Got cancel: ID: " << input.order_id << std::endl;
        cancelOrder(it->second, input_time);
      }
      else {
        Output::OrderDeleted(input.order_id, false, input_time,
                          CurrentTimestamp());
      }
      break;
    }
    default:
      if(input.count<=0) {
        std::cout << "The order count must be larger than zero." << std::endl;;
        break;
      }
      // std::cout << "Got order: " << static_cast<char>(input.type) << " "
      //           << input.instrument << " x " << input.count << " @ "
      //           << input.price << " ID: " << input.order_id
      //           << std::endl;
     //Output::OrderAdded(input.order_id, input.instrument, input.price,
                 //        input.count, input.type == input_sell,
                  //       input_time, CurrentTimestamp());
      if (input.type == input_buy)
      {
        manageBuyOrders(input, input_time);
      }
      else
      {
        manageSellOrders(input, input_time);
      }
      break;
    }
  }
}